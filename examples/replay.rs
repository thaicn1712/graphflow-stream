use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use graph_flow::{Context, NextAction, Task, TaskResult, error::Result};
use graphflow_stream::{StreamEvent, emit_token, record, spawn_task};

struct Greeting;

#[async_trait]
impl Task for Greeting {
    fn id(&self) -> &str {
        "greeting"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        for word in ["Hello", ",", " ", "world", "!"] {
            emit_token("greeting", word).await;
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        Ok(TaskResult::new(
            Some("Hello, world!".to_string()),
            NextAction::Continue,
        ))
    }
}

#[tokio::main]
async fn main() {
    println!("recording a run...");
    let (rx, handle) = spawn_task(Arc::new(Greeting), Context::new(), 16);
    let recording = record(rx).await;
    handle.await.unwrap().unwrap();

    let json = serde_json::to_string_pretty(&recording).unwrap();
    println!(
        "recorded {} events, {} bytes as JSON\n",
        recording.events.len(),
        json.len()
    );

    println!("replaying (same timing as the original run, no LLM call this time)...");
    let restored: graphflow_stream::Recording = serde_json::from_str(&json).unwrap();
    let mut rx = restored.replay(16);
    while let Some(event) = rx.recv().await {
        if let StreamEvent::Token { delta, .. } = event {
            print!("{delta}");
        }
    }
    println!();
}
