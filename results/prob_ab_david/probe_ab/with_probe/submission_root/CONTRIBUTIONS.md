# Team Contributions

The following table records our agreed main responsibilities. Implementation,
testing, integration, and documentation also involved shared work across the team.

| Member | Main responsibilities | Experiments/report sections | Agreed contribution % |
|---|---|---|---:|
| David Goh | Create test cases and set up the test environment; investigate system behavior and diagnose issues. Support integration and refactoring of the implementation. | Experiment setup, probe versus no-probe comparison, VM/virtual-environment configuration, and interpretation of system behavior and test results. | 25% |
| Hai Chen | Explain and refine server-to-client communication, including the mechanisms used to keep delivery responsive: independent senders, bounded buffers, shared final-log replay, lag probes, catch-up pacing, and slow-client isolation. | Server-to-client delivery, backpressure and overload policy, recovery ordering, and regression checks for slow or stalled receivers. | 25% |
| Ng Kheng Yang | Explain client-to-server communication and client behavior; prepare sequence diagrams and client documentation covering initial connection, late join, live updates, local recall, temporary disconnection, and reconnect recovery. Improve browser behavior and catch-up handling. | Client handshake and finalized resume markers, component interaction sequences, browser revision/rendering behavior, and client interaction patterns. | 25% |
| Wang Yanxiang | Explain `app/server.py`, the roles of the project files, and the behavior of the server and caption source. Document server coordination, revision validation, and recovery; implement and explain the optional ASR source and demonstration. | Architecture and protocol, server/source walkthrough, revision and recovery semantics, bounded history responses, and optional ASR setup and demo. | 25% |
| **Total** | | | **100%** |

## Shared Work

The team discussed and agreed on the system design together before dividing the
workload. Each member then implemented and documented their corresponding parts
based on the shared design.
