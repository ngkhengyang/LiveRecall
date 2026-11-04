# Group 6 - LiveRecall

LiveRecall distributes revision-based captions over WebSockets to browser
clients, with local 15/30/60-second recall and automatic reconnect/recovery.
The mandatory application uses the supplied deterministic caption source.
Local speech recognition is an optional input source.

## Team members

| Member | Student ID | Main responsibility |
|---|---|---|
| David Goh Zhe Kai | A0192324J | Test environment, experiments, integration, and results analysis |
| Qin Haichen | A0214409L | Server-to-client delivery, backpressure, and slow-client isolation |
| Ng Kheng Yang | A0277167R | Browser behavior, reconnect recovery, and interaction diagrams |
| Wang Yanxiang | A0281803H | Server/source coordination, protocol documentation, and optional ASR |

See [the contribution record](CONTRIBUTIONS.md) for the agreed work split.

## Repository layout

- `submission_root/`: required server entry point, `app/` implementation,
  browser client, and submission documents.
- `test/` and `network_profiles/`: public deterministic harness and Ubuntu
  network impairment scripts.
- `experiments/` and `results/`: probe comparison runner and saved evidence.
- `bonus_asr/`: optional local speech-recognition adapter.
- `docs/`: report source and supporting guides.

Run the commands below from the **repository root**, the directory containing
`submission_root/`, `test/`, and `bonus_asr/`, unless stated otherwise.
The test, experiment, ASR, and documentation paths require those sibling
directories; they are not included in a standalone `submission_root/` copy.

## Ubuntu setup

Experiments used Ubuntu version 26.04.1 on virtual box and Python 3.12.3. Use **Python 3.10 or newer**

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -c 'import sys; assert sys.version_info >= (3, 10), "LiveRecall requires Python 3.10+"'
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

If you received only the submission directory, run these commands from inside
it instead. Serve `client/` in a second terminal using the same environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python solution_server.py --port 8765
# Second terminal, also inside the submission directory:
.venv/bin/python -m http.server 8000 --directory client
```

For browsers outside the server's machine or VM, replace `127.0.0.1` with its
reachable IP address in both the HTTP URL and the `?ws=` override.

## Design notes

- **Revision state:** the server and browser keep the latest revision per
  segment, reject duplicate or older revisions, and never replace a final.
  The browser updates one row per segment.
- **Bounded history:** the shared final log retains 90 seconds of accepted
  finals; reconnect and late-join requests return at most 60 seconds of recent
  finals. Unfinished server state is capped at 4,000 segments. History memory
  depends on source rate and caption size within the retention window.
- **Recovery:** the browser resumes after its latest finalized
  `(segment, revision)` marker. The server captures context and a log boundary
  together, sends the context first, then delivers subsequent updates. This
  assumes increasing segment IDs and ordered finalization, as in the supplied
  source. Existing unfinished captions are absent from the recovery snapshot.

Reconnect and late-join context arrives as ordered `HISTORY` messages, targeting
at most 64 captions and 256 KiB of complete JSON per message. All snapshot
messages precede live updates. Empty context still receives one empty `HISTORY`.
A caption larger than the byte target travels alone without truncation and must
still fit the receiving client's message-size limit.

- **Backpressure:** each receiver has an independent sender and FIFO. Queue
  pressure (256 pending updates or the 4 MiB byte limit) or a late ping/pong
  probe switches that receiver to shared-log final replay and keeps only the
  latest queued draft per unfinished segment. Probe-driven catch-up uses paced
  bursts; healthy receivers retain normal revision delivery. Live replay uses
  individual `CAPTION_UPDATE` messages.
- **Overload trade-off:** intermediate drafts may be skipped during catch-up.
  Remaining drafts must fit 4,000 entries and 4 MiB of pending JSON. Capacity
  failure, expired outstanding finals, or a 75-second send/probe stall closes
  that receiver with code 1013. These bounds exclude transport buffers and
  recovery snapshots. Reconnecting recovers only the requested recent window,
  so sustained overload does not guarantee complete delivery.
- **Local recall:** the browser's 15/30/60-second view reads locally held
  finals, with a 30-second default. Final state is pruned at 150 seconds on
  accepted updates; unfinished browser state is capped at 400 segments.

Use one source/session at a time. Restart the server and reload browser tabs
before starting a new lecture or restarting segment numbering; source
disconnection does not clear retained state.

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
(
  set -e
  trap 'sudo bash network_profiles/reset.sh lo' EXIT
  sudo bash network_profiles/delay_loss.sh lo
  .venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t5
)
```

See [the public-test guide](../docs/PUBLIC_TESTS.md) for metrics and the required
before/after comparison. Recorded runs are linked below.

## Final submission check

The final checker requires `README.md`, `AI_USAGE.md`, `CONTRIBUTIONS.md`, and
`report.pdf` inside `submission_root/`, alongside the server, `app/`, requirements,
and browser files. Fill in Ng Kheng Yang's pending student ID in the team
details before submission.

```bash
.venv/bin/python test/submission_check.py --final
```

This checks required files and the server's WebSocket startup; it does not
validate revision semantics, recovery correctness, or performance thresholds.
Exclude virtual environments, cached models, and generated `output/` files
from the submission package.

To reproduce the T1-T4 probe comparison using the environment above:

```bash
.venv/bin/python -u experiments/probe_ab.py --tests t1,t2,t3,t4 --runs 3 \
  --venv "$PWD/.venv" --port 8811 --out output/reproduction_t1_t4
```

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
