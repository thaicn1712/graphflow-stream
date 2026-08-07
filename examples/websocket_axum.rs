use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use graph_flow::{error::Result, Context, NextAction, Task, TaskResult};
use graphflow_stream::{emit_token, spawn_task, StreamEvent};

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

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    let (mut rx, _handle) = spawn_task(Arc::new(Greeting), Context::new(), 16);

    while let Some(event) = rx.recv().await {
        let text = match event {
            StreamEvent::TaskStarted { task_id } => format!("started {task_id}"),
            StreamEvent::Token { task_id, delta } => format!("token {task_id} {delta}"),
            StreamEvent::TaskFinished { task_id } => format!("finished {task_id}"),
            StreamEvent::TaskFailed { task_id, error } => format!("failed {task_id} {error}"),
        };
        if socket.send(Message::Text(text.into())).await.is_err() {
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/ws", get(ws_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3001")
        .await
        .unwrap();
    println!("listening on ws://127.0.0.1:3001/ws");
    axum::serve(listener, app).await.unwrap();
}
