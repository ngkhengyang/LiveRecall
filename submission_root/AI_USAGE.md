# AI / Coding-Assistant Usage

## Tools used

We used OpenAI Codex to assist with:

- **Writing documentation:** drafting and revising setup instructions, protocol explanations, technical documentation, and report text based on our implementation and results.
- **Generating test cases:** producing test scenarios and regression checks for caption revision ordering, duplicate/stale rejection, reconnect recovery, slow readers, and queue limits.
- **Generating code from our system design:** implementing and refining components based on the team's architecture, delivery rules, and protocol requirements.
- **Making the ASR demo video:** assisting with the optional ASR adapter, demo setup, and preparation of the demonstration video showing generated captions, recall, reconnect recovery, and late join.

## One useful suggestion

One useful suggestion was to use a per-client WebSocket ping/pong probe to
detect receivers falling behind and activate catch-up delivery. A completed
socket send alone does not establish that the receiver is keeping up, because
data can remain buffered in the transport. Detecting lag allows the server to
reduce superseded provisional updates for that receiver while continuing
normal revision delivery to healthy clients.

The current implementation in `app/client.py` uses the probe and queue pressure
to switch a receiver from normal FIFO delivery into replay mode. Finals are
read from the shared caption log, while the receiver's buffer keeps the latest
unfinished state for each active segment. The receiver returns to normal
delivery after it catches up and its catch-up lease expires.

We evaluated the probe using the saved with-probe versus no-probe experiments
in `results/probe_ab/SUMMARY.md`, with three independent runs per configuration.
For the T3 slow-reader workload, the recorded medians were:

| Metric | Without probe | With probe |
|---|---:|---:|
| Final completeness | 95.13% | 99.38% |
| Slow-client P95 caption age | 28,015.74 ms | 8,470.33 ms |
| Normal-client P95 caption age | 38.58 ms | 29.02 ms |

These measurements apply to the saved experiment configurations. They show
the benefit under that slow-reader workload; they do not establish an
improvement for every workload. Catch-up also trades intermediate revision
coverage for faster delivery of useful caption state to lagging receivers.

## One suggestion that was wrong, inefficient, or required modification

An earlier generated implementation always kept only one pending update per
segment. Each newer revision replaced the previous queued revision, including
when the final caption arrived, even under normal conditions. This could leave
only the final caption to send when several revisions arrived before the
sender drained its queue.

Although this reduced queued work, it violated normal update coverage: healthy
clients must receive every accepted provisional revision followed by its
final, in arrival order. Replacing the displayed caption row with its latest
revision is appropriate in the browser, but dropping intermediate revisions
before sending them prevents the client from receiving the revision sequence.

We corrected this by preserving each accepted revision as a separate FIFO
entry during normal delivery and enabling coalescing only after receiver lag
or queue pressure is detected. The change is recorded in commit `323a9fc`
("Preserve caption revisions until client overload is detected") and remains
part of the current `app/outbound_buffer.py` delivery policy.

Regression cases in `tests/test_delivery.py` check that a normal burst delivers
every revision in global arrival order, that a final cannot overtake its
provisional revisions, and that full revision delivery resumes after queue
pressure clears.

## Responsibility statement

We understand that the team remains responsible for the submitted code,
experimental results, and claims, and that every member may be asked to
explain or modify relevant code during the viva.
