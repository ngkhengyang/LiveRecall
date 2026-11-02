#!/usr/bin/env python3
"""Recognize a playing local lecture and publish the standard caption protocol.

Run from the repository root: python -m bonus_asr.asr_source lecture.mp4 --play
Optional ASR dependencies are imported only when recognition starts.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
from pathlib import Path
import sys
import time
from typing import Awaitable, Callable

# Reuse the server's protocol definitions.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "submission_root"))
from app.protocol import CaptionUpdate, SourceDone, SourceHello

SAMPLE_RATE = 16000
# One audio channel; each sample is stored as a signed 2-byte value.
BYTES_PER_SECOND = SAMPLE_RATE * 2


async def read_pcm(reader: asyncio.StreamReader, byte_count: int) -> bytes:
    """Keep short pipe reads together; preserve the final incomplete chunk."""
    try:
        data = await reader.readexactly(byte_count)
    except asyncio.IncompleteReadError as exc:
        data = exc.partial
    if len(data) % 2:
        raise RuntimeError("Decoder returned an incomplete 16-bit audio sample")
    return data


async def stream_captions(
    reader: asyncio.StreamReader,
    recognize: Callable[[bytes], str],
    emit: Callable[[dict], Awaitable[None]],
    *,
    session: str = "L1",
    start_segment: int = 1,
    chunk_seconds: float = 5.0,
    partial_seconds: float = 2.5,
    start_offset: float = 0.0,
    playback_started: float | None = None,
) -> int:
    """Recognize one audio chunk at a time.

    Recognize the first part of each chunk to send a provisional caption.
    Then recognize the full chunk to send a final caption that may correct it.
    Skip empty results unless a provisional caption needs a final update.
    """
    chunk_bytes = int(chunk_seconds * SAMPLE_RATE) * 2
    partial_bytes = int(partial_seconds * SAMPLE_RATE) * 2
    segment = start_segment
    processed_bytes = 0
    completed = 0

    while True:
        audio = await read_pcm(reader, partial_bytes or chunk_bytes)
        if not audio:
            break
        audio_start = start_offset + processed_bytes / BYTES_PER_SECOND
        revision = 0

        async def publish(text: str, final: bool, inference_seconds: float) -> None:
            nonlocal revision
            revision += 1
            audio_end = audio_start + len(audio) / BYTES_PER_SECOND
            event = CaptionUpdate(
                session, segment, revision, time.time(), final, text
            ).to_dict()
            event.update(
                audio_start=audio_start,
                audio_end=audio_end,
                recognition_seconds=inference_seconds,
            )
            if playback_started is not None:
                event["playback_lag_seconds"] = max(
                    0.0,
                    time.monotonic() - playback_started - (audio_end - start_offset),
                )
            await emit(event)

        if partial_bytes and len(audio) == partial_bytes:
            started = time.monotonic()
            text = await asyncio.to_thread(recognize, audio)
            if text.strip():
                await publish(text.strip(), False, time.monotonic() - started)
            audio += await read_pcm(reader, chunk_bytes - len(audio))

        started = time.monotonic()
        text = (await asyncio.to_thread(recognize, audio)).strip()
        if text or revision:
            # Send a final update even if the text is empty, clearing any earlier
            # incorrect provisional text and marking the segment as complete.
            await publish(text, True, time.monotonic() - started)
            completed += 1
            segment += 1
        processed_bytes += len(audio)

    return completed


def build_decoder_command(args, ffmpeg: str) -> list[str]:
    command = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-re"]
    command += ["-ss", str(args.start_seconds), "-i", str(args.media.resolve())]
    if args.duration is not None:
        command += ["-t", str(args.duration)]
    return command + [
        "-map",
        "0:a:0",
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(SAMPLE_RATE),
        "-acodec",
        "pcm_s16le",
        "-f",
        "s16le",
        "pipe:1",
    ]


def build_player_command(args, ffplay: str) -> list[str]:
    command = [
        ffplay,
        "-hide_banner",
        "-loglevel",
        "error",
        "-autoexit",
        "-ss",
        str(args.start_seconds),
    ]
    if args.duration is not None:
        command += ["-t", str(args.duration)]
    return command + ["-sn", "-i", str(args.media.resolve())]


async def stop_process(process) -> None:
    if process is None:
        return
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
    # Read any remaining output so a full pipe cannot block cleanup.
    try:
        await asyncio.wait_for(process.communicate(), timeout=3)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.communicate()


def load_recognizer(args):
    import numpy as np
    from faster_whisper import WhisperModel

    model = WhisperModel(
        args.model,
        device=args.device,
        compute_type=args.compute_type,
        cpu_threads=args.cpu_threads,
        download_root=str(args.model_cache),
        local_files_only=args.local_files_only,
    )

    def recognize(pcm: bytes) -> str:
        audio = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768.0
        segments, _ = model.transcribe(
            audio,
            language=None if args.language == "auto" else args.language,
            beam_size=1,
            vad_filter=True,
            condition_on_previous_text=False,
        )
        # Reading these results runs speech recognition. Finish that work in
        # this background thread so WebSocket messages can still be processed.
        return " ".join(part.text.strip() for part in segments).strip()

    return recognize


async def run(args, recognize: Callable[[bytes], str]) -> int:
    import websockets

    decoder = player = None
    try:
        async with websockets.connect(args.uri, max_size=2**20) as ws:
            await ws.send(SourceHello(args.session).to_json())
            playback_started = time.monotonic()
            if args.play:
                player = await asyncio.create_subprocess_exec(
                    *build_player_command(args, "ffplay"),
                    stdin=asyncio.subprocess.DEVNULL,
                )
            decoder = await asyncio.create_subprocess_exec(
                *build_decoder_command(args, args.ffmpeg or "ffmpeg"),
                stdout=asyncio.subprocess.PIPE,
                limit=64 * 1024,
            )

            async def emit(event: dict) -> None:
                if player is not None and player.returncode not in (None, 0):
                    raise RuntimeError(
                        f"Video player exited with code {player.returncode}"
                    )
                message = json.dumps(event, allow_nan=False)
                await ws.send(message)
                print(message, flush=True)

            completed = await stream_captions(
                decoder.stdout,
                recognize,
                emit,
                session=args.session,
                start_segment=args.start_segment,
                chunk_seconds=args.chunk_seconds,
                partial_seconds=args.partial_seconds,
                start_offset=args.start_seconds,
                playback_started=playback_started,
            )
            code = await decoder.wait()
            if code:
                raise RuntimeError(f"Audio decoder failed ({code})")
            if player is not None and player.returncode not in (None, 0):
                raise RuntimeError(f"Video player exited with code {player.returncode}")
            await ws.send(SourceDone(args.session, completed).to_json())
            print(f"ASR complete: {completed} finalized segments", flush=True)
            return completed
    finally:
        await stop_process(decoder)
        await stop_process(player)


def finite_number(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise argparse.ArgumentTypeError("must be a finite number")
    return number


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("media", type=Path, help="Local lecture video/audio file")
    parser.add_argument("--uri", default="ws://127.0.0.1:8765")
    parser.add_argument("--session", default="L1")
    parser.add_argument("--start-segment", type=int, default=1)
    parser.add_argument(
        "--model", default="base.en", help="Model name or local model directory"
    )
    parser.add_argument("--model-cache", type=Path, default=Path("output/asr-models"))
    parser.add_argument("--local-files-only", action="store_true")
    parser.add_argument("--device", choices=["cpu", "cuda", "auto"], default="cpu")
    parser.add_argument("--compute-type", default="int8")
    parser.add_argument("--cpu-threads", type=int, default=4)
    parser.add_argument("--language", default="en", help="Language code, or auto")
    parser.add_argument("--chunk-seconds", type=finite_number, default=5.0)
    parser.add_argument(
        "--partial-seconds",
        type=finite_number,
        default=2.5,
        help="0 disables provisional recognition",
    )
    parser.add_argument("--start-seconds", type=finite_number, default=0.0)
    parser.add_argument(
        "--duration",
        type=finite_number,
        help="Excerpt length; use at least 180 for the bonus demo",
    )
    parser.add_argument(
        "--play", action="store_true", help="Play the same excerpt with ffplay"
    )
    parser.add_argument("--ffmpeg", help="Optional decoder executable path")
    args = parser.parse_args(argv)
    if not args.media.is_file():
        parser.error("media must be an existing local video/audio file")
    if not 0.1 <= args.chunk_seconds <= 30:
        parser.error("--chunk-seconds must be between 0.1 and 30")
    if not 0 <= args.partial_seconds < args.chunk_seconds:
        parser.error("--partial-seconds must be >= 0 and less than --chunk-seconds")
    if args.start_seconds < 0 or (args.duration is not None and args.duration <= 0):
        parser.error("start must be >= 0 and duration must be > 0")
    if args.start_segment < 1 or args.cpu_threads < 1 or not args.session.strip():
        parser.error("segment/threads must be positive and session must be nonempty")
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    try:
        print(f"Loading ASR model {args.model}...", flush=True)
        recognize = load_recognizer(args)
        asyncio.run(run(args, recognize))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
