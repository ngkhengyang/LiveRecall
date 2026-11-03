# Caption timing investigation

Run the usual run_demo.py command. Each run now also writes
`<output-stem>_runNN_source.jsonl`, even with --quiet. No test durations,
source pacing, expected counts, or percentile calculations are changed.

Each SOURCE_SEND row identifies a segment/revision and records:
- source_ts: epoch seconds assigned immediately before serialization/send.
- send_start_mono / send_complete_mono: monotonic seconds around ws.send.
- send_complete_ts: epoch seconds after ws.send returns.
- send_duration_ms: local serialization/send duration, using monotonic time.
SOURCE_FINISHED records the time after SOURCE_DONE and source connection close.
Send completion means local WebSocket send completed, not server receipt or broadcast.
The file is buffered to reduce measurement overhead and closes on normal exit or
Python exceptions; forced termination can lose buffered rows.

The client CSV adds CLIENT_START, RECEIVE_STOP (before the normal duration-end
close handshake), and CLIENT_STOP (after the client finishes). These include
monotonic_ts and the shared deadline_mono. Existing receive_ts remains epoch
seconds. Forced disconnects retain their existing FORCED_DISCONNECT row.

Compare source send_start_mono with each client's deadline_mono and RECEIVE_STOP
monotonic_ts. All processes must run on the same machine/VM for this comparison.
For each missing (segment, revision), compare source send timing with that
client's stop time, rather than the time of its last received caption:
- Source starts sending after RECEIVE_STOP: emitted after that client stopped reading.
- Source starts before the deadline but send completes after it: source send spans
  the deadline; send_duration_ms shows local send/backpressure time.
- Send completes well before RECEIVE_STOP but no matching client row: a delivery,
  buffering, or processing problem remains possible. Server tracing is needed to
  distinguish server receipt, broadcast, drop, and client processing delays.
Compare successive revision-1 send starts to measure segment-start spacing.
Do not use SOURCE_FINISHED alone as the last caption's emission time.

P99 is computed by analyse_results.py from accepted live CAPTION_UPDATE rows:
`latency_ms = (receive_ts - source_ts) * 1000`.
receive_ts is sampled immediately after await ws.recv(), before JSON parsing and
classification. source_ts originates at the source and is preserved by the server.
The analyser sorts these values and linearly interpolates at (N - 1) * 0.99.
All received accepted live revisions across clients are pooled; HISTORY,
duplicate/stale rows and missing captions are excluded. It is not server-only
latency, not time between segments, and not a percentile of per-client P99s.
Source serialization/send, transport, server handling/queuing, and client receive
scheduling can contribute. Pre-emission generation delays and captions never
received do not contribute. Epoch-clock adjustments can affect this metric.
Completeness must therefore be examined alongside P99.

Logging adds some overhead. This instrumentation does not attribute an unreceived
caption's delay to the server without additional server-side evidence.

## Sleep timing

SOURCE_SLEEP records every existing revision delay and segment gap, without
changing the requested delays or adding catch-up scheduling. Each record names
the preceding segment/revision and phase, with sleep_start_mono, sleep_end_mono,
requested_ms, actual_ms, and overshoot_ms (actual minus requested).

Sum overshoot_ms over the intervals before a late emission to see how much drift
occurred inside sleeps. Compare remaining gaps between SOURCE_SEND and
SOURCE_SLEEP boundaries to find time spent outside those awaits, including
logging, preparation, and process scheduling. The final segment gap still occurs
as in the original source; exclude it when explaining the last caption's lateness.
An overrun means the source coroutine resumed late; it does not by itself identify
whether the cause was guest scheduling, host scheduling, or other event-loop work.
Buffered logging can still occasionally write to disk, so instrumentation has cost.
