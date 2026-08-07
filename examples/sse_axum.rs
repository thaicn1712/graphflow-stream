use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
};
use futures_core::Stream;
use graph_flow::{Context, NextAction, Task, TaskResult, error::Result};
use graphflow_stream::{StreamEvent, emit_token, spawn_task};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

struct Greeting;

#[async_trait::async_trait]
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

async fn stream_handler()
-> Sse<impl Stream<Item = std::result::Result<Event, std::convert::Infallible>>> {
    let (rx, _handle) = spawn_task(Arc::new(Greeting), Context::new(), 16);
    let stream = ReceiverStream::new(rx).map(|event| {
        let data = match event {
            StreamEvent::TaskStarted { task_id } => format!("started {task_id}"),
            StreamEvent::Token { task_id, delta } => format!("token {task_id} {delta}"),
            StreamEvent::TaskFinished { task_id } => format!("finished {task_id}"),
            StreamEvent::TaskFailed { task_id, error } => format!("failed {task_id} {error}"),
        };
        Ok(Event::default().data(data))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/stream", get(stream_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("listening on http://127.0.0.1:3000/stream");
    axum::serve(listener, app).await.unwrap();
}
