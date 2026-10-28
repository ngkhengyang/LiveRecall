# CS3103 Assignment 4 - LiveRecall Starter Package

This package gives you a **working naive baseline** plus public test tools. It is not a complete graded solution.

The baseline is intended to let you see `test/caption_source.py -> submission_root/solution_server.py -> browser/load clients` working immediately. The networking mechanisms that distinguish strong submissions remain TODOs.

## What is already provided

- required `python solution_server.py --port <port>` startup path from `submission_root/`;
- basic WebSocket source/client connection handling;
- naive direct broadcast of caption updates;
- deterministic caption generator;
- scalable simulated clients and CSV logs;
- public metrics and independent-run support;
- minimal visible browser UI;
- Ubuntu `tc/netem` examples.

## What is deliberately NOT solved

- revision replacement / deduplication;
- bounded recovery history;
- correct late join and reconnect recovery;
- bounded per-client application queues;
- slow-client isolation;
- semantic coalescing and backpressure;
- constrained-bandwidth overload policy;
- final usable browser behavior.

## Quick start

```bash
make install
```

Basic public experiment:

```bash
make demo
```

For results used in the report, use three independent runs:

```bash
make report-demo
```

See `PUBLIC_TESTS.md` for T1-T5.

## Browser demo

See `DEMO_GUIDE.md`. The starter browser intentionally appends each revision as a new line; this makes the revision-replacement TODO obvious.

## Short background references

- `PROTOCOL_EXAMPLES.md` - normal, reconnect, and late-join message flow.
- `NETWORKING_PRIMER.md` - WebSocket/TCP, asyncio, per-client queues, bounded state, and backpressure terminology.

These guides explain mechanics, not the optimal design.

## Submission compatibility

During development:

```bash
make check
```

Before submission, after you have created the required final files:

```bash
make check-final
```

The instructor reference implementation and hidden evaluator are not included. Do not modify the public measurement logic when reporting the required public-test results.
