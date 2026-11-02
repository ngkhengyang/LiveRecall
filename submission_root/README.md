# LiveRecall

LiveRecall distributes revision-based captions over WebSockets to browser
clients, with local 15/30/60-second recall and automatic reconnect/recovery.
The mandatory application uses the supplied deterministic caption source.
Local speech recognition is an optional input source.

Run the commands below from the **repository root**, the directory containing
`submission_root/`, `test/`, and `bonus_asr/`, unless stated otherwise.

## Ubuntu setup

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv .venv
.venv/bin/python -m pip install -r submission_root/requirements.txt
```

The mandatory server requires only the dependencies in
`submission_root/requirements.txt`. It does not require ASR packages, model
weights, or an external service.

## Server and browser client

Start the server in one terminal:

```bash
.venv/bin/python submission_root/solution_server.py --port 8765
```

Serve the browser client in a second terminal:

```bash
.venv/bin/python -m http.server 8000 --directory submission_root/client
```

Open `http://127.0.0.1:8000` in two browser tabs or windows. For a different
WebSocket port, use a URL such as
`http://127.0.0.1:8000/?ws=ws://127.0.0.1:9000`.

The required evaluator entry point also works from `submission_root/` after
activating the server environment:

```bash
source .venv/bin/activate
cd submission_root
python solution_server.py --port 8765
```

Return to the repository root before following the remaining sections.

## Mandatory deterministic application demo

With the server and two browser clients running, start the supplied source in
a third terminal:

```bash
.venv/bin/python test/caption_source.py --duration 120
```

Show provisional captions being replaced by finals in the same row, local
recall with the default 30-second view and 15/30/60-second options, a temporary
disconnect using the browser's simulation control, automatic recovery without
duplicate finals, and a late-joining browser receiving recent context.

## Public deterministic tests

Stop any manually started server and caption source before using the harness;
it starts its own server. Run these experiments inside Ubuntu, with the server
and load clients in the same environment. Each command uses three independent
runs and reports median results.

```bash
.venv/bin/python test/submission_check.py
.venv/bin/python test/run_demo.py --clients 20 --duration 20 --runs 3 --output-stem output/t1
.venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t2
.venv/bin/python test/run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem output/t3
.venv/bin/python test/run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem output/t4
```

For T5, apply the supplied network profile, run the experiment, and reset the
profile afterwards even if the experiment fails:

```bash
sudo bash network_profiles/delay_loss.sh lo
.venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t5
sudo bash network_profiles/reset.sh lo
```

These are reproduction commands, not claims that the experiments have been
completed. See [the public-test guide](../docs/PUBLIC_TESTS.md) for metrics and
the required before/after comparison.

## Optional ASR demo

The optional adapter uses local `faster-whisper` inference on a lecture video's
audio. It sends `SOURCE_HELLO`, `CAPTION_UPDATE`, and `SOURCE_DONE` through the
same server and browser used for the deterministic application. Captions are
recognized during playback; they are not loaded from a subtitle file.
Include `bonus_asr/` alongside `submission_root/` when packaging this demo.

### Separate ASR environment

Keep these dependencies separate from the mandatory server environment:

```bash
sudo apt install -y ffmpeg
python3 -m venv .venv-asr
.venv-asr/bin/python -m pip install -r bonus_asr/requirements.txt
```

The Ubuntu `ffmpeg` package provides the audio decoder and `ffplay` video
player; install it before running the adapter. Playback requires a
desktop/display. A headless Ubuntu VM can run recognition without `--play`,
but that alone does not demonstrate lecture-video playback.

Download the default English model before the demonstration. This preparation
step needs Internet access; the demo command below then uses cached weights:

```bash
.venv-asr/bin/python -c "from faster_whisper import WhisperModel; WhisperModel('base.en', device='cpu', compute_type='int8', download_root='output/asr-models')"
```

Do not include virtual environments or model weights in the submission.

### Run a continuous three-minute lecture excerpt

Use an existing local lecture video with at least 180 seconds of content.
Replace `/path/to/lecture.mp4` below with its path; a YouTube page URL is not
accepted directly by the adapter.

Stop the deterministic source, restart the server using the command above,
and reload both browser tabs. Then run:

```bash
.venv-asr/bin/python -m bonus_asr.asr_source /path/to/lecture.mp4 \
  --play --duration 180 --local-files-only
```

The model loads before playback starts. To choose a later excerpt, add
`--start-seconds 60` and ensure the video contains at least another 180 seconds
from that point. For a custom server port, add
`--uri ws://127.0.0.1:9000` and use the matching browser URL.

Record at least three continuous minutes of lecture playback alongside the
submitted browser showing generated captions. Demonstrate revision replacement,
local recall, and reconnect/recovery. This bonus demonstration is additional to
the compulsory deterministic application demonstration.

Use one source and session `L1` at a time. Restart the server and reload the
browsers for a new lecture because the server retains caption state when a
source disconnects. To deliberately continue the same lecture, use
`--start-segment N` with a number above its previous highest segment ID.
Ctrl+C stops the adapter and its decoder/player processes.

### Timing and limitations

By default, each five-second audio window produces a provisional result from
its first 2.5 seconds and a final result from the full window. Recognition and
audio buffering add delay; playback and decoding start at approximately the
same time, without frame-accurate synchronization. Embedded subtitles are
disabled in the player.

If inference cannot keep up, bounded buffering backpressures the decoder while
video playback continues, so caption lag can grow. `--partial-seconds 0`
disables provisional inference to reduce computation. The selected 180 seconds
of media can take longer than 180 seconds to finish recognizing.

`source_ts` records caption emission time. Network Caption Age therefore
excludes audio buffering and recognition time. Adapter logs separately include
audio offsets, recognition duration, and approximate playback lag. Fixed chunk
boundaries can split words or sentences, and recognition accuracy is not
guaranteed.
