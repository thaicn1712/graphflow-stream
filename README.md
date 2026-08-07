# graphflow-stream

Token-level streaming for [`graph-flow`](https://crates.io/crates/graph-flow) agent graphs — the missing piece between [Rig](https://crates.io/crates/rig-core)'s streaming LLM completions and `graph-flow`'s LangGraph-style step executor.

## The gap

`graph-flow` gives Rust a LangGraph-style graph engine: stateful workflows, checkpointing (in-memory + Postgres), human-in-the-loop pauses via `WaitForInput`, conditional edges, fan-out. `Rig` gives Rust streaming LLM completions. Neither bridges the two: `graph_flow::Task::run` resolves to one final `TaskResult` — there is no way for a task to hand a caller partial output while it's still running, the way `astream_events` works in LangGraph.

That means today, an agent graph built on `graph-flow` + `rig-core` cannot stream a token-by-token response to a user. Every response arrives all at once, after the task has already finished.

## What this crate adds

A `StreamingTask` trait that sits alongside `graph_flow::Task`:

```rust
#[async_trait]
pub trait StreamingTask: Task {
    async fn run_streaming(&self, context: Context, tx: StreamSender) -> Result<TaskResult>;
}
```

It forwards `StreamEvent`s (`TaskStarted`, `Token { delta }`, `TaskFinished`, `TaskFailed`) over a channel while the task runs, but still returns the exact same `TaskResult` your graph's `NextAction` control flow (`Continue` / `GoTo` / `WaitForInput` / `End`) already depends on. Nothing about `graph-flow`'s executor, storage, or graph building changes — this is additive, not a fork.

```rust
use graphflow_stream::{spawn_streaming_task, StreamEvent};
use std::sync::Arc;

let (mut rx, handle) = spawn_streaming_task(Arc::new(my_llm_task), context, 32);

while let Some(event) = rx.recv().await {
    if let StreamEvent::Token { delta, .. } = event {
        // forward `delta` over SSE / a websocket / stdout as it arrives
    }
}

let task_result = handle.await??; // same TaskResult your graph already expects
```

For tasks you haven't converted yet, wrap them in `NonStreaming<T>` — it implements `StreamingTask` for any existing `Task`, emitting only start/finish/fail events around a single `run()` call, so a graph can mix streaming and non-streaming tasks while you migrate incrementally.

## Status

Early scaffold (v0.1.0): the core trait, event type, and a standalone `spawn_streaming_task` runner are implemented and tested against `graph-flow` 0.6.0's real API. Not yet wired into `graph-flow`'s own `FlowRunner`/`GraphBuilder` — right now you call `run_streaming` yourself for whichever step you know is a streaming step. That integration (driving an entire graph run with per-step streaming, not just a single task) is the next milestone.

Roadmap:
- [ ] Drive a full graph run (`GraphBuilder`/`FlowRunner`) with per-task streaming, not just a single task
- [ ] Helper for bridging a `rig_core` streaming completion directly into `StreamEvent::Token`s
- [ ] Subgraph composition (nested graphs) — currently unsupported by `graph-flow` itself
- [ ] SSE/axum example

## Why this exists

Python's AI agent ecosystem (LangGraph, CrewAI, AutoGen) is years ahead of Rust's. `graph-flow` and `rig-core` are closing that gap fast, but streaming — arguably the single most user-visible feature of any LLM app — is a real hole between them. This crate exists to close it.

## License

MIT
