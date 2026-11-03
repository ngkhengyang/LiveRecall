# LiveRecall: component interaction sequence

The diagram follows the current implementation in `submission_root/`.
Components are responsibilities inside one Python server and each browser.
The internal final-log cursor is separate from the receiver's wire
`last_segment/last_revision` resume marker.

```mermaid
sequenceDiagram
    participant Source as Caption source
    participant Server as Server / Source handler
    participant Store as CaptionStore
    participant Client as Receiver Client
    participant Buffer as OutboundBuffer
    participant Sender as Sender task
    participant Probe as Probe task / LagPolicy
    participant Browser as Browser Connection / CaptionState / View

    Source->>Server: SOURCE_HELLO(session)
    Browser->>Server: CLIENT_HELLO(finalized marker, history_seconds)
    Server->>Client: Construct and run
    Client->>Server: Register receiver
    Client->>Store: Capture requested finals and snapshot_tail
    Note over Client,Store: No network await between registration and snapshot
    loop Ordered recovery batches, or one empty batch
        Client->>Browser: HISTORY(items)
        Browser->>Browser: Accept newer finals and render local state
    end
    Client->>Client: Commit captured tail and release history helper locals
    Client->>Sender: Start task
    Client->>Probe: Start task

    loop Accepted source revisions
        Source->>Server: CAPTION_UPDATE(segment, revision, final, text, source_ts)
        Server->>Store: validate_and_apply
        alt Stale, duplicate, post-final, or retired ID
            Store-->>Server: Reject
        else Accepted
            Store-->>Server: Accepted typed caption
            Server->>Client: enqueue without awaiting receiver sockets
            alt NORMAL
                Client->>Buffer: offer with FIFO admission
                alt Pressure or admission failure
                    Client->>Buffer: Coalesce once; drop queued finals; retry if needed
                    Client->>Client: Enter REPLAY
                end
            else REPLAY
                Client->>Buffer: Keep latest active, or remove active when final arrives
            end
            Client->>Sender: Wake
        end
    end

    par Independent sending
        loop Pending local work or replay
            alt NORMAL
                Sender->>Buffer: Pop oldest FIFO revision
                Buffer-->>Sender: Caption
                Sender->>Store: Resolve final sequence if caption is final
            else REPLAY
                Sender->>Buffer: Try ready active after four finals
                opt No active selected ahead of finals
                    Sender->>Store: read_next_final(committed cursor)
                    Store-->>Sender: Next final and sequence, None, or ReplayExpired
                end
                opt No final is owed
                    Sender->>Buffer: Try ready active state
                end
                Note over Sender,Buffer: Latest active state uses one-second first-enqueue grace
            end
            alt Caption selected
                Sender->>Browser: Individual CAPTION_UPDATE
                Browser->>Browser: Accept revision; replace segment row
                Sender->>Sender: Commit cursor only after successful send
            else No caption ready and log caught up
                Note over Sender,Buffer: When lease expires, resume FIFO<br/>while retaining young active entries
            end
        end
    and Connection lag observation
        loop One outstanding whole ping/pong
            Probe->>Browser: Ping
            alt Deadline expires at max(0.5 seconds, 6 × RTT)
                Probe->>Client: Enter REPLAY and renew sticky lease
                Client->>Buffer: Coalesce once and release queued finals
            end
            Browser-->>Probe: Pong when transport/library can answer
            Probe->>Sender: Completed-probe notification
            Note over Probe,Sender: During lease, one burst before next hold<br/>64 messages / 256 KiB targets; pong is not an application ACK
        end
    end

    Browser->>Browser: Recall last 15 / 30 / 60 seconds from local finals
    Note over Browser: Normal recall sends no server request

    Browser--xClient: Connection lost or simulated disconnect
    Client->>Sender: Cancel and await
    Client->>Probe: Cancel and await
    Client->>Server: Unregister
    Browser->>Browser: Keep local caption state; apply reconnect back-off
    Browser->>Server: CLIENT_HELLO with latest accepted final marker
    Note over Client,Browser: Repeat registration, bounded HISTORY, then live delivery
```

Each send and whole probe has a 75-second terminal stall budget. Closing gets
two seconds before transport abort. Expired owed finals and active-state
admission failure close explicitly; the server does not silently skip required
finals. Reconnect covers eligible retained context rather than a permanent
delivery session.

The diagram shows logical concurrency. Source validation and fan-out are
synchronous on the server event loop; independent sends do not remove shared
CPU, memory, or bandwidth limits.
