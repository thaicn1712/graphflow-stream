use std::sync::Arc;

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use graph_flow::{Context, NextAction, Task, TaskResult, error::Result};
use graphflow_stream::{
    DynamicMapTask, EnsembleTask, emit_token, majority_vote, record, spawn_task,
};
use tokio::runtime::Runtime;

struct Silent;

#[async_trait]
impl Task for Silent {
    fn id(&self) -> &str {
        "silent"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        Ok(TaskResult::new(None, NextAction::Continue))
    }
}

struct Chatty {
    tokens: usize,
}

#[async_trait]
impl Task for Chatty {
    fn id(&self) -> &str {
        "chatty"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        for _ in 0..self.tokens {
            emit_token("chatty", "x").await;
        }
        Ok(TaskResult::new(None, NextAction::Continue))
    }
}

fn bench_baseline_no_streaming(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("task.run() direct, no graphflow-stream involved", |b| {
        b.to_async(&rt)
            .iter(|| async { Silent.run(Context::new()).await.unwrap() });
    });
}

fn bench_emit_with_no_listener(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("emit_token() with nobody listening (ambient no-op)", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..100 {
                emit_token("bench", "x").await;
            }
        });
    });
}

fn bench_spawn_task_with_listener(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function(
        "spawn_task() streaming 100 tokens to a draining receiver",
        |b| {
            b.to_async(&rt).iter_batched(
                || Arc::new(Chatty { tokens: 100 }),
                |task| async move {
                    let (mut rx, handle) = spawn_task(task, Context::new(), 128);
                    while rx.recv().await.is_some() {}
                    handle.await.unwrap().unwrap()
                },
                BatchSize::SmallInput,
            );
        },
    );
}

fn bench_record_100_tokens(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("record() capturing 100 streamed tokens", |b| {
        b.to_async(&rt).iter_batched(
            || Arc::new(Chatty { tokens: 100 }),
            |task| async move {
                let (rx, handle) = spawn_task(task, Context::new(), 128);
                let recording = record(rx).await;
                handle.await.unwrap().unwrap();
                recording
            },
            BatchSize::SmallInput,
        );
    });
}

struct MapItem {
    item: String,
}

#[async_trait]
impl Task for MapItem {
    fn id(&self) -> &str {
        &self.item
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        Ok(TaskResult::new(
            Some(self.item.clone()),
            NextAction::Continue,
        ))
    }
}

fn bench_dynamic_map_task_10_items(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let context = Context::new();
    context
        .set(
            "items",
            (0..10).map(|i| format!("item_{i}")).collect::<Vec<_>>(),
        )
        .unwrap();
    let map_task = DynamicMapTask::new("map", |ctx: &Context| {
        let items: Vec<String> = ctx.get("items").unwrap_or_default();
        items
            .into_iter()
            .map(|item| Arc::new(MapItem { item }) as Arc<dyn Task>)
            .collect()
    });

    c.bench_function("DynamicMapTask::run, 10 items", |b| {
        b.to_async(&rt)
            .iter(|| async { map_task.run(context.clone()).await.unwrap() });
    });
}

fn bench_ensemble_task_5_runs(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ensemble = EnsembleTask::new("ensemble", Silent, 5, majority_vote);

    c.bench_function("EnsembleTask::run, 5 runs", |b| {
        b.to_async(&rt)
            .iter(|| async { ensemble.run(Context::new()).await.unwrap() });
    });
}

criterion_group!(
    benches,
    bench_baseline_no_streaming,
    bench_emit_with_no_listener,
    bench_spawn_task_with_listener,
    bench_record_100_tokens,
    bench_dynamic_map_task_10_items,
    bench_ensemble_task_5_runs
);
criterion_main!(benches);
