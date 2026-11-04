# LiveRecall communication sequence

This overview follows the current implementation in `submission_root/`.
Server includes source routing and shared `CaptionStore` access, each receiver
has a separate `Client`, buffer, sender and probe. These are responsibilities
inside one asyncio process, not separate services.
For task-level replay and probe interactions, see [the detailed component
sequence](sequence.md). The browser resume marker (`last_segment`,
`last_revision`) is distinct from the server's internal final-log cursor.

```mermaid
sequenceDiagram
    participant Source as Caption source
    participant Server as Server / CaptionStore
    participant Delivery as Per-receiver Client
    participant Browser as Student browser

    Source->>Server: SOURCE_HELLO(session)
    Browser->>Server: CLIENT_HELLO(last_segment, last_revision,<br/>history_seconds)
    Server->>Delivery: Construct and run receiver
    Delivery->>Server: Register, capture requested finals and<br/>snapshot tail
    Note over Server,Delivery: No yield between registration and snapshot
    Delivery->>Browser: Ordered HISTORY batches, or one empty<br/>batch
    Delivery->>Delivery: Set internal cursor to snapshot tail,<br/>start sender and probe

    loop Accepted source revisions
        Source->>Server: CAPTION_UPDATE(segment, revision, final,<br/>text, source_ts)
        Server->>Server: Validate, apply newer state, retain finals
        Server->>Delivery: Enqueue for each receiver without socket<br/>waits
        Note over Server,Delivery: Independent sender tasks, FIFO normally<br/>Pressure or late probes: replay shared<br/>finals and latest active state
        Delivery->>Browser: CAPTION_UPDATE, commit final cursor after<br/>successful send
        Browser->>Browser: Reject stale/final changes, replace<br/>segment row
    end
    Browser->>Browser: Recall local finals for 15 / 30 / 60<br/>seconds

    Browser--xDelivery: Connection lost
    Delivery->>Delivery: Cancel and await sender/probe tasks
    Delivery->>Server: Unregister receiver
    Note over Source,Server: Source and delivery to other receivers<br/>continue
    Browser->>Browser: Retain local state, retry with capped<br/>back-off
    Browser->>Server: CLIENT_HELLO(latest final marker,<br/>history_seconds=60)
    Server->>Delivery: Construct a new receiver, repeat snapshot<br/>procedure
    Delivery->>Browser: Missed retained finals in HISTORY, then<br/>live updates
    Browser->>Browser: Merge newer finals without duplicate rows
```

Initial history is sent directly by `Client` before sender/probe tasks start,
it is not queued in `OutboundBuffer`. Updates arriving during history are
buffered, and finals beyond the captured tail remain owed. Ping/pong probes
measure transport progress, not caption consumption. Finals are retained for
90 seconds, hello recovery requests at most 60 seconds. The finalized browser
marker assumes ordered completion and cannot represent holes below a later final.
