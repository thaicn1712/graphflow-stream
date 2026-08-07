use std::sync::Arc;

use async_trait::async_trait;
use graph_flow::{
    Context, FlowRunner, GraphBuilder, InMemorySessionStorage, NextAction, Session, SessionStorage,
    Task, TaskResult, error::Result,
};
use graphflow_stream::{
    StreamEvent, SubgraphTask, collect_text, emit_finished, emit_started, emit_token, spawn_graph,
    spawn_task,
};

struct Echo;

#[async_trait]
impl Task for Echo {
    fn id(&self) -> &str {
        "echo"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        emit_started("echo").await;
        emit_token("echo", "hel").await;
        emit_token("echo", "lo").await;
        emit_finished("echo").await;
        Ok(TaskResult::new(
            Some("hello".to_string()),
            NextAction::Continue,
        ))
    }
}

#[tokio::test]
async fn spawn_task_forwards_ambient_emits_and_preserves_result() {
    let task = Arc::new(Echo);
    let (mut rx, handle) = spawn_task(task, Context::new(), 8);

    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }

    let result = handle.await.unwrap().unwrap();
    assert_eq!(result.response, Some("hello".to_string()));
    assert_eq!(
        events,
        vec![
            StreamEvent::TaskStarted {
                task_id: "echo".to_string()
            },
            StreamEvent::Token {
                task_id: "echo".to_string(),
                delta: "hel".to_string()
            },
            StreamEvent::Token {
                task_id: "echo".to_string(),
                delta: "lo".to_string()
            },
            StreamEvent::TaskFinished {
                task_id: "echo".to_string()
            },
        ]
    );
}

#[tokio::test]
async fn emit_without_a_scope_is_a_silent_noop() {
    emit_token("orphan", "nobody is listening").await;
}

#[tokio::test]
async fn collect_text_joins_token_deltas_in_order() {
    let text = collect_text(Arc::new(Echo), Context::new(), 8)
        .await
        .unwrap();
    assert_eq!(text, "hello");
}

struct Greeter;

#[async_trait]
impl Task for Greeter {
    fn id(&self) -> &str {
        "greeter"
    }

    async fn run(&self, context: Context) -> Result<TaskResult> {
        emit_token("greeter", "Hi").await;
        context.set("greeted", true).unwrap();
        Ok(TaskResult::new(None, NextAction::Continue))
    }
}

struct Farewell;

#[async_trait]
impl Task for Farewell {
    fn id(&self) -> &str {
        "farewell"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        emit_token("farewell", "Bye").await;
        Ok(TaskResult::new(Some("done".to_string()), NextAction::End))
    }
}

fn greeting_graph(id: &str) -> graph_flow::Graph {
    GraphBuilder::new(id)
        .add_task(Arc::new(Greeter))
        .add_task(Arc::new(Farewell))
        .add_edge("greeter", "farewell")
        .set_start_task("greeter")
        .build()
        .unwrap()
}

#[tokio::test]
async fn spawn_graph_streams_tokens_across_multiple_tasks() {
    let graph = greeting_graph("greeting");

    let storage: Arc<dyn SessionStorage> = Arc::new(InMemorySessionStorage::new());
    let session = Session::new_from_task("s1".to_string(), "greeter");
    storage.save(session).await.unwrap();

    let runner = FlowRunner::new(Arc::new(graph), storage);
    let (mut rx, handle) = spawn_graph(runner, "s1", 16);

    let mut deltas = Vec::new();
    while let Some(event) = rx.recv().await {
        if let StreamEvent::Token { delta, .. } = event {
            deltas.push(delta);
        }
    }

    let result = handle.await.unwrap().unwrap();
    assert_eq!(deltas, vec!["Hi".to_string(), "Bye".to_string()]);
    assert_eq!(result.response, Some("done".to_string()));
}

#[tokio::test]
async fn subgraph_task_runs_inner_graph_and_streams_through_outer_scope() {
    let inner = greeting_graph("inner");
    let subgraph = Arc::new(SubgraphTask::new("subgraph", Arc::new(inner)));

    let (mut rx, handle) = spawn_task(subgraph, Context::new(), 16);

    let mut deltas = Vec::new();
    while let Some(event) = rx.recv().await {
        if let StreamEvent::Token { delta, .. } = event {
            deltas.push(delta);
        }
    }

    let result = handle.await.unwrap().unwrap();
    assert_eq!(deltas, vec!["Hi".to_string(), "Bye".to_string()]);
    assert_eq!(result.response, Some("done".to_string()));
    assert_eq!(result.next_action, NextAction::Continue);
}
