# graphflow-stream

[![crates.io](https://img.shields.io/crates/v/graphflow-stream.svg?style=flat)](https://crates.io/crates/graphflow-stream)
[![docs.rs](https://docs.rs/graphflow-stream/badge.svg?style=flat)](https://docs.rs/graphflow-stream)
[![CI](https://github.com/thaicn1712/graphflow-stream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/thaicn1712/graphflow-stream/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/graphflow-stream.svg?style=flat)](LICENSE)

The `astream_events` LangGraph gives Python, for [`graph-flow`](https://crates.io/crates/graph-flow) in Rust.

## Install

```bash
cargo add graphflow-stream
```

## Usage

```rust,ignore
use graph_flow::{Context, NextAction, Task, TaskResult, error::Result};
use graphflow_stream::{emit_token, spawn_task};

struct MyLlmTask;

#[async_trait::async_trait]
impl Task for MyLlmTask {
    fn id(&self) -> &str { "my_llm_task" }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        for delta in ["Hel", "lo", "!"] {           // e.g. deltas from Rig
            emit_token("my_llm_task", delta).await;
        }
        Ok(TaskResult::new(Some("Hello!".into()), NextAction::Continue))
    }
}

let (mut rx, handle) = spawn_task(Arc::new(MyLlmTask), Context::new(), 32);
while let Some(event) = rx.recv().await {
    // forward over SSE / WebSocket / stdout as it arrives
}
let result = handle.await??; // same TaskResult you'd get from task.run()
```

Just want the full streamed text back, no manual loop? One line:

```rust,ignore
let text = graphflow_stream::collect_text(Arc::new(MyLlmTask), Context::new(), 32).await?;
```

`emit_token`/`emit_started`/`emit_finished`/`emit_failed` are ambient — call them from anywhere inside `Task::run`, no new trait to implement, no-op if nothing is listening. `spawn_graph(flow_runner, session_id, buffer)` does the same for a whole `FlowRunner` run; `SubgraphTask` wraps a nested `Graph` as one `Task` and streams through automatically.

Replay debugging: `record(rx).await` turns a run into a `Recording` (serializable, so it can be saved to disk), and `recording.replay(buffer)` plays it back on a fresh channel with the original timing — inspect a past run, or demo a UI without hitting an LLM again.

## Orchestration: map over a runtime list, vote across runs

`graph_flow`'s built-in `FanOutTask` runs a fixed set of children decided at construction time. `DynamicMapTask` covers what LangGraph's `Send` API covers in Python — fan out over however many items `context` holds *this run* (one child per retrieved document, one per subtask an LLM just planned):

```rust,ignore
use graphflow_stream::DynamicMapTask;

let map_task = DynamicMapTask::new("summarize_retrieved", |ctx: &Context| {
    let docs: Vec<String> = ctx.get("retrieved_docs").unwrap_or_default();
    docs.into_iter()
        .map(|doc_id| Arc::new(SummarizeDoc { doc_id }) as Arc<dyn Task>)
        .collect()
}).with_prefix("summaries");

map_task.run(context).await?; // writes summaries.<doc_id>.response for each doc
```

`EnsembleTask` runs the same task several times concurrently and reduces the responses — self-consistency prompting, sample an LLM call a few times and combine instead of trusting one draw:

```rust,ignore
use graphflow_stream::{EnsembleTask, majority_vote};

let ensemble = EnsembleTask::new("classify_intent", ClassifyIntent, 5, majority_vote);
let result = ensemble.run(context).await?; // most common of 5 concurrent runs
```

`majority_vote` ships built in; pass any `Fn(Vec<String>) -> String` for a custom reducer (join, longest, an LLM-as-judge pick).

More examples (`full_graph`, `sse_axum`, `websocket_axum`, `replay`, `map_and_ensemble`) in [`examples/`](examples).

## Benchmarks

`cargo bench` (criterion, [`benches/overhead.rs`](benches/overhead.rs)):

| Scenario | Time |
|---|---|
| `task.run()` direct — no `graphflow-stream` involved | ~1.0 µs |
| `emit_token()` with nobody listening (ambient no-op) | ~30 ns / call |
| `spawn_task()` streaming 100 tokens to a draining receiver | ~2.0 µs / token |
| `record()` capturing 100 streamed tokens | ~1.4 µs / token |
| `DynamicMapTask::run`, 10 items | ~239 µs |
| `EnsembleTask::run`, 5 runs | ~215 µs |

## License

MIT
