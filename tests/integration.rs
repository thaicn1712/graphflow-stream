use std::sync::Arc;

use async_trait::async_trait;
use graph_flow::{
    Context, FlowRunner, GraphBuilder, InMemorySessionStorage, NextAction, Session, SessionStorage,
    Task, TaskResult, error::Result,
};
use graphflow_stream::{
    DynamicMapTask, EnsembleTask, StreamEvent, SubgraphTask, collect_text, emit_finished,
    emit_started, emit_token, majority_vote, record, spawn_graph, spawn_task,
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

#[tokio::test]
async fn recording_round_trips_through_json_and_replays_in_order() {
    let task = Arc::new(Echo);
    let (rx, handle) = spawn_task(task, Context::new(), 16);

    let recording = record(rx).await;
    handle.await.unwrap().unwrap();
    assert_eq!(recording.events.len(), 4);

    let json = serde_json::to_string(&recording).unwrap();
    let restored: graphflow_stream::Recording = serde_json::from_str(&json).unwrap();

    let mut rx = restored.replay(16);
    let mut replayed = Vec::new();
    while let Some(event) = rx.recv().await {
        replayed.push(event);
    }
    assert_eq!(
        replayed,
        recording
            .events
            .into_iter()
            .map(|r| r.event)
            .collect::<Vec<_>>()
    );
}

struct SummarizeItem {
    item: String,
}

#[async_trait]
impl Task for SummarizeItem {
    fn id(&self) -> &str {
        &self.item
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        Ok(TaskResult::new(
            Some(format!("summary of {}", self.item)),
            NextAction::Continue,
        ))
    }
}

fn summarize_all_docs(context: &Context) -> Vec<Arc<dyn Task>> {
    let docs: Vec<String> = context.get("docs").unwrap_or_default();
    docs.into_iter()
        .map(|item| Arc::new(SummarizeItem { item }) as Arc<dyn Task>)
        .collect()
}

#[tokio::test]
async fn dynamic_map_task_fans_out_over_a_runtime_list_and_aggregates() {
    let context = Context::new();
    context
        .set(
            "docs",
            vec![
                "doc_a".to_string(),
                "doc_b".to_string(),
                "doc_c".to_string(),
            ],
        )
        .unwrap();

    let map_task =
        DynamicMapTask::new("summarize_all", summarize_all_docs).with_prefix("summaries");

    let result = map_task.run(context.clone()).await.unwrap();
    assert_eq!(result.next_action, NextAction::Continue);

    let a: Option<String> = context.get("summaries.doc_a.response");
    let b: Option<String> = context.get("summaries.doc_b.response");
    let c: Option<String> = context.get("summaries.doc_c.response");
    assert_eq!(a, Some("summary of doc_a".to_string()));
    assert_eq!(b, Some("summary of doc_b".to_string()));
    assert_eq!(c, Some("summary of doc_c".to_string()));
}

#[tokio::test]
async fn dynamic_map_task_scales_with_however_many_items_are_in_context() {
    let map_task = DynamicMapTask::new("summarize_all", summarize_all_docs);

    let one_doc = Context::new();
    one_doc.set("docs", vec!["only".to_string()]).unwrap();
    let result = map_task.run(one_doc).await.unwrap();
    assert!(result.response.unwrap().contains("mapped over 1 item"));

    let five_docs = Context::new();
    five_docs
        .set("docs", (0..5).map(|i| format!("d{i}")).collect::<Vec<_>>())
        .unwrap();
    let result = map_task.run(five_docs).await.unwrap();
    assert!(result.response.unwrap().contains("mapped over 5 item"));
}

struct VotingTask {
    counter: Arc<std::sync::atomic::AtomicUsize>,
    answers: Vec<&'static str>,
}

#[async_trait]
impl Task for VotingTask {
    fn id(&self) -> &str {
        "voting_task"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        let i = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let answer = self.answers[i % self.answers.len()];
        Ok(TaskResult::new(
            Some(answer.to_string()),
            NextAction::Continue,
        ))
    }
}

#[tokio::test]
async fn ensemble_task_reduces_multiple_runs_via_majority_vote() {
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let inner = VotingTask {
        counter,
        answers: vec!["yes", "yes", "no"],
    };
    let task = EnsembleTask::new("vote", inner, 3, majority_vote);

    let result = task.run(Context::new()).await.unwrap();
    assert_eq!(result.response, Some("yes".to_string()));
}

#[tokio::test]
async fn ensemble_task_runs_at_least_once_even_if_zero_requested() {
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let inner = VotingTask {
        counter: counter.clone(),
        answers: vec!["only"],
    };
    let task = EnsembleTask::new("vote", inner, 0, majority_vote);

    let result = task.run(Context::new()).await.unwrap();
    assert_eq!(result.response, Some("only".to_string()));
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ensemble_task_with_custom_reducer_joins_responses() {
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let inner = VotingTask {
        counter,
        answers: vec!["a", "b"],
    };
    let task = EnsembleTask::new("join", inner, 2, |responses: Vec<String>| {
        responses.join(",")
    });

    let result = task.run(Context::new()).await.unwrap();
    let response = result.response.unwrap();
    assert!(response.contains('a') && response.contains(','));
}
