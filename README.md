# graphflow-stream

Token-level streaming for [`graph-flow`](https://crates.io/crates/graph-flow) agent graphs — the missing piece between [Rig](https://crates.io/crates/rig-core)'s streaming LLM completions and `graph-flow`'s LangGraph-style step executor.

## The gap

`graph-flow` gives Rust a LangGraph-style graph engine: stateful workflows, checkpointing (in-memory + Postgres), human-in-the-loop pauses via `WaitForInput`, conditional edges, fan-out. `Rig` gives Rust streaming LLM completions. Neither bridges the two: `graph_flow::Task::run` and `FlowRunner::run` both resolve to a single final value once a task (or a full graph step) is done — there is no way to observe partial output while execution is still in flight, the way `astream_events` works in LangGraph.

## What this crate adds

An ambient, task-local event channel. Call `emit_token`/`emit_started`/`emit_finished`/`emit_failed` from anywhere inside a `Task::run` implementation — no new trait to implement, no changes to `graph-flow`'s `Task`, `Graph`, or `FlowRunner`:

```rust
use graph_flow::{Context, NextAction, Task, TaskResult, error::Result};
use graphflow_stream::emit_token;

struct MyLlmTask;

#[async_trait::async_trait]
impl Task for MyLlmTask {
    fn id(&self) -> &str { "my_llm_task" }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        // stream_completion() yields text deltas from your provider (e.g. Rig)
        for delta in ["Hel", "lo", "!"] {
            emit_token("my_llm_task", delta).await;
        }
        Ok(TaskResult::new(Some("Hello!".into()), NextAction::Continue))
    }
}
```

`emit_*` calls are a no-op if nothing is listening — a task works the same whether or not it's driven through this crate. To actually receive the events, drive the task through one of:

- **`spawn_task(task, context, buffer)`** — run a single `Task`, get back a `StreamReceiver` plus a `JoinHandle<Result<TaskResult>>`.
- **`spawn_graph(flow_runner, session_id, buffer)`** — drive a `graph_flow::FlowRunner` to completion (looping through every task in the graph), streaming every task's `emit_*` calls through one channel.
- **`run_streaming(buffer, future)`** — the primitive both of the above are built on, if you need something else.

```rust
let (mut rx, handle) = graphflow_stream::spawn_graph(flow_runner, session_id, 32);

while let Some(event) = rx.recv().await {
    // forward event over SSE / WebSocket / stdout as it arrives
}

let execution_result = handle.await??; // same ExecutionResult FlowRunner already returns
```

### Subgraph composition

`SubgraphTask` wraps an entire `graph_flow::Graph` as a single `Task`, so it can be a node in an outer graph. Since it's driven through the same ambient channel, any `emit_*` call from a task inside the subgraph is forwarded transparently through the outer scope — no special-casing needed:

```rust
let subgraph = SubgraphTask::new("summarizer", Arc::new(inner_graph));
outer_graph_builder.add_task(Arc::new(subgraph));
```

### Bridging an arbitrary text stream

`forward_text_stream(task_id, stream)` drains anything implementing `futures_core::Stream<Item = String>` into `emit_token` calls — the shape you'd map a Rig `StreamingResult` into (extract the text delta from each `StreamedAssistantContent`/`RawStreamingChoice` chunk, yield it as a `String`), or any other provider's streaming response.

### Other transports (WebSocket, gRPC, WebRTC, ...)

`StreamReceiver` is a plain `tokio::sync::mpsc::Receiver<StreamEvent>` — it isn't tied to HTTP or SSE. Mapping it onto another transport is the same handful of lines as the SSE example: drain the receiver and forward each `StreamEvent` in whatever shape that transport wants (a WebSocket text/binary frame, a gRPC server-streaming response item, a WebRTC data-channel message). See `examples/websocket_axum.rs` for the WebSocket version; gRPC/WebRTC aren't included as examples here since they pull in `tonic`/`webrtc` and protobuf codegen that most consumers of this crate won't need, but the bridging pattern is identical.

## Examples

```bash
cargo run --example full_graph     # streams a two-task graph to stdout, no server
cargo run --example sse_axum       # then: curl -N http://127.0.0.1:3000/stream
cargo run --example websocket_axum # then connect a WebSocket client to ws://127.0.0.1:3001/ws
```

## Status

v0.1.0. Tested against `graph-flow` 0.6.0's real API (single-task streaming, full-graph streaming, and subgraph composition all have integration tests in `tests/integration.rs`).

Roadmap:
- [x] Stream a full graph run (`FlowRunner`), not just a single task
- [x] Subgraph composition (nested graphs)
- [x] Generic bridge from any `Stream<Item = String>` (covers Rig and other providers)
- [x] Transport examples (SSE, WebSocket)
- [ ] Time-travel / replay debugging over recorded `StreamEvent`s

## Why this exists

Python's AI agent ecosystem (LangGraph, CrewAI, AutoGen) is years ahead of Rust's. `graph-flow` and `rig-core` are closing that gap fast, but streaming — arguably the single most user-visible feature of any LLM app — was a real hole between them. This crate closes it.

## License

MIT
