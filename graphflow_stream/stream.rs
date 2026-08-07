#![doc = include_str!("../README.md")]

use graph_flow::{
    Context, ExecutionResult, ExecutionStatus, FlowRunner, Graph, InMemorySessionStorage,
    NextAction, Session, SessionStorage, Task, TaskResult,
    error::{GraphError, Result},
};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub type StreamSender = mpsc::Sender<StreamEvent>;
pub type StreamReceiver = mpsc::Receiver<StreamEvent>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    TaskStarted { task_id: String },
    Token { task_id: String, delta: String },
    TaskFinished { task_id: String },
    TaskFailed { task_id: String, error: String },
}

tokio::task_local! {
    static STREAM_TX: StreamSender;
}

pub async fn emit(event: StreamEvent) {
    if let Ok(tx) = STREAM_TX.try_with(|tx| tx.clone()) {
        let _ = tx.send(event).await;
    }
}

pub async fn emit_started(task_id: impl Into<String>) {
    emit(StreamEvent::TaskStarted {
        task_id: task_id.into(),
    })
    .await;
}

pub async fn emit_token(task_id: impl Into<String>, delta: impl Into<String>) {
    emit(StreamEvent::Token {
        task_id: task_id.into(),
        delta: delta.into(),
    })
    .await;
}

pub async fn emit_finished(task_id: impl Into<String>) {
    emit(StreamEvent::TaskFinished {
        task_id: task_id.into(),
    })
    .await;
}

pub async fn emit_failed(task_id: impl Into<String>, error: impl Into<String>) {
    emit(StreamEvent::TaskFailed {
        task_id: task_id.into(),
        error: error.into(),
    })
    .await;
}

pub async fn forward_text_stream<S>(task_id: impl Into<String>, mut stream: S)
where
    S: futures_core::Stream<Item = String> + Unpin,
{
    use tokio_stream::StreamExt;

    let task_id = task_id.into();
    while let Some(delta) = stream.next().await {
        emit_token(task_id.clone(), delta).await;
    }
    emit_finished(task_id).await;
}

pub fn run_streaming<F>(buffer: usize, fut: F) -> (StreamReceiver, JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = mpsc::channel(buffer);
    let handle = tokio::spawn(STREAM_TX.scope(tx, fut));
    (rx, handle)
}

pub fn spawn_task<T>(
    task: Arc<T>,
    context: Context,
    buffer: usize,
) -> (StreamReceiver, JoinHandle<Result<TaskResult>>)
where
    T: Task + Send + Sync + 'static,
{
    run_streaming(buffer, async move { task.run(context).await })
}

pub async fn collect_text<T>(task: Arc<T>, context: Context, buffer: usize) -> Result<String>
where
    T: Task + Send + Sync + 'static,
{
    let (mut rx, handle) = spawn_task(task, context, buffer);
    let mut text = String::new();
    while let Some(event) = rx.recv().await {
        if let StreamEvent::Token { delta, .. } = event {
            text.push_str(&delta);
        }
    }
    handle.await.map_err(|e| GraphError::Other(e.into()))??;
    Ok(text)
}

pub fn spawn_graph(
    flow_runner: FlowRunner,
    session_id: impl Into<String>,
    buffer: usize,
) -> (StreamReceiver, JoinHandle<Result<ExecutionResult>>) {
    let session_id = session_id.into();
    run_streaming(buffer, async move {
        run_to_completion(&flow_runner, &session_id).await
    })
}

async fn run_to_completion(flow_runner: &FlowRunner, session_id: &str) -> Result<ExecutionResult> {
    loop {
        let result = flow_runner.run(session_id).await?;
        if !matches!(result.status, ExecutionStatus::Paused { .. }) {
            return Ok(result);
        }
    }
}

static SUBGRAPH_SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct SubgraphTask {
    id: String,
    graph: Arc<Graph>,
    storage: Arc<dyn SessionStorage>,
}

impl SubgraphTask {
    pub fn new(id: impl Into<String>, graph: Arc<Graph>) -> Self {
        Self {
            id: id.into(),
            graph,
            storage: Arc::new(InMemorySessionStorage::new()),
        }
    }

    pub fn with_storage(mut self, storage: Arc<dyn SessionStorage>) -> Self {
        self.storage = storage;
        self
    }
}

#[async_trait::async_trait]
impl Task for SubgraphTask {
    fn id(&self) -> &str {
        &self.id
    }

    async fn run(&self, context: Context) -> Result<TaskResult> {
        let start_task_id = self.graph.start_task_id().ok_or_else(|| {
            GraphError::TaskNotFound(format!("subgraph '{}' has no start task", self.graph.id))
        })?;

        let suffix = SUBGRAPH_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let session_id = format!("{}:{}", self.id, suffix);

        let mut session = Session::new_from_task(session_id.clone(), start_task_id)
            .with_graph_id(self.graph.id.clone());
        session.context = context;
        self.storage.save(session).await?;

        let runner = FlowRunner::new(self.graph.clone(), self.storage.clone());
        let result = run_to_completion(&runner, &session_id).await?;

        let next_action = match result.status {
            ExecutionStatus::WaitingForInput => NextAction::WaitForInput,
            _ => NextAction::Continue,
        };
        Ok(TaskResult::new(result.response, next_action))
    }
}
