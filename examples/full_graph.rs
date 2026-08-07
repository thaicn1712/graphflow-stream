use std::sync::Arc;

use async_trait::async_trait;
use graph_flow::{
    Context, FlowRunner, GraphBuilder, InMemorySessionStorage, NextAction, Session, SessionStorage,
    Task, TaskResult, error::Result,
};
use graphflow_stream::{StreamEvent, emit_token, spawn_graph};

struct Draft;

#[async_trait]
impl Task for Draft {
    fn id(&self) -> &str {
        "draft"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        for word in ["Once", " upon", " a", " time"] {
            emit_token("draft", word).await;
        }
        Ok(TaskResult::new(None, NextAction::Continue))
    }
}

struct Polish;

#[async_trait]
impl Task for Polish {
    fn id(&self) -> &str {
        "polish"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        for word in [",", " there", " was", " a", " crate."] {
            emit_token("polish", word).await;
        }
        Ok(TaskResult::new(
            Some("Once upon a time, there was a crate.".to_string()),
            NextAction::End,
        ))
    }
}

#[tokio::main]
async fn main() {
    let graph = GraphBuilder::new("story")
        .add_task(Arc::new(Draft))
        .add_task(Arc::new(Polish))
        .add_edge("draft", "polish")
        .set_start_task("draft")
        .build()
        .unwrap();

    let storage: Arc<dyn SessionStorage> = Arc::new(InMemorySessionStorage::new());
    storage
        .save(Session::new_from_task("demo".to_string(), "draft"))
        .await
        .unwrap();

    let runner = FlowRunner::new(Arc::new(graph), storage);
    let (mut rx, handle) = spawn_graph(runner, "demo", 16);

    while let Some(event) = rx.recv().await {
        if let StreamEvent::Token { delta, .. } = event {
            print!("{delta}");
        }
    }
    println!();

    let result = handle.await.unwrap().unwrap();
    println!("final response: {:?}", result.response);
}
