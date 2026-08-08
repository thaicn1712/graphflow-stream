#![doc = include_str!("../README.md")]

use graph_flow::{
    Context, ExecutionResult, ExecutionStatus, FlowRunner, Graph, InMemorySessionStorage,
    NextAction, Session, SessionStorage, Task, TaskResult,
    error::{GraphError, Result},
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Sending half of a stream channel; a running task's `emit_*` calls write here.
pub type StreamSender = mpsc::Sender<StreamEvent>;
/// Receiving half of a stream channel; drain this to observe a run in progress.
pub type StreamReceiver = mpsc::Receiver<StreamEvent>;

/// An incremental event emitted while a task or graph run is in flight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamEvent {
    /// A task started running.
    TaskStarted { task_id: String },
    /// A partial chunk of output (e.g. one LLM token).
    Token { task_id: String, delta: String },
    /// A task finished successfully.
    TaskFinished { task_id: String },
    /// A task returned an error instead of finishing normally.
    TaskFailed { task_id: String, error: String },
}

tokio::task_local! {
    static STREAM_TX: StreamSender;
}

/// Send an event on the ambient stream, if one is active; a no-op otherwise.
pub async fn emit(event: StreamEvent) {
    if let Ok(tx) = STREAM_TX.try_with(|tx| tx.clone()) {
        let _ = tx.send(event).await;
    }
}

/// Emit a [`StreamEvent::TaskStarted`] on the ambient stream.
pub async fn emit_started(task_id: impl Into<String>) {
    emit(StreamEvent::TaskStarted {
        task_id: task_id.into(),
    })
    .await;
}

/// Emit a [`StreamEvent::Token`] on the ambient stream.
pub async fn emit_token(task_id: impl Into<String>, delta: impl Into<String>) {
    emit(StreamEvent::Token {
        task_id: task_id.into(),
        delta: delta.into(),
    })
    .await;
}

/// Emit a [`StreamEvent::TaskFinished`] on the ambient stream.
pub async fn emit_finished(task_id: impl Into<String>) {
    emit(StreamEvent::TaskFinished {
        task_id: task_id.into(),
    })
    .await;
}

/// Emit a [`StreamEvent::TaskFailed`] on the ambient stream.
pub async fn emit_failed(task_id: impl Into<String>, error: impl Into<String>) {
    emit(StreamEvent::TaskFailed {
        task_id: task_id.into(),
        error: error.into(),
    })
    .await;
}

/// Drain any `Stream<Item = String>` (e.g. a mapped LLM completion) into `emit_token` calls.
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

/// Run `fut` with an ambient stream scope active; the primitive `spawn_task`/`spawn_graph` use.
pub fn run_streaming<F>(buffer: usize, fut: F) -> (StreamReceiver, JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = mpsc::channel(buffer);
    let handle = tokio::spawn(STREAM_TX.scope(tx, fut));
    (rx, handle)
}

/// Run a single [`Task`], streaming its `emit_*` calls out through the returned receiver.
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

/// Run a [`Task`] and join its streamed [`StreamEvent::Token`] deltas into one `String`.
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

/// Drive a [`FlowRunner`] to completion, streaming every task's `emit_*` calls out.
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

/// A [`Task`] that drives a whole inner [`Graph`], so a graph can be a node in another graph.
pub struct SubgraphTask {
    id: String,
    graph: Arc<Graph>,
    storage: Arc<dyn SessionStorage>,
}

impl SubgraphTask {
    /// Wrap `graph` as a task with the given id, using in-memory session storage.
    pub fn new(id: impl Into<String>, graph: Arc<Graph>) -> Self {
        Self {
            id: id.into(),
            graph,
            storage: Arc::new(InMemorySessionStorage::new()),
        }
    }

    /// Use a custom [`SessionStorage`] backend for the inner graph's sessions.
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

/// A [`StreamEvent`] plus its offset from the start of the recording.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedEvent {
    pub event: StreamEvent,
    pub at: Duration,
}

/// A recorded run: every [`StreamEvent`] a `StreamReceiver` produced, with timing.
///
/// Serializable, so a recording can be saved and replayed later — e.g. to debug a
/// past run, or demo a UI without hitting an LLM again.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Recording {
    pub events: Vec<RecordedEvent>,
}

impl Recording {
    /// Replay this recording on a fresh channel, preserving the original timing between events.
    pub fn replay(self, buffer: usize) -> StreamReceiver {
        let (tx, rx) = mpsc::channel(buffer);
        tokio::spawn(async move {
            let mut last = Duration::ZERO;
            for recorded in self.events {
                let wait = recorded.at.saturating_sub(last);
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                last = recorded.at;
                if tx.send(recorded.event).await.is_err() {
                    return;
                }
            }
        });
        rx
    }
}

/// Drain a `StreamReceiver` into a [`Recording`], timestamping each event from the start.
pub async fn record(mut rx: StreamReceiver) -> Recording {
    let start = std::time::Instant::now();
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(RecordedEvent {
            event,
            at: start.elapsed(),
        });
    }
    Recording { events }
}

fn map_key(prefix: &Option<String>, child_id: &str, field: &str) -> String {
    match prefix {
        Some(p) => format!("{p}.{child_id}.{field}"),
        None => format!("map.{child_id}.{field}"),
    }
}

/// Fans out over a list of child tasks built from `context` at run time — the
/// same shape as `graph_flow::fanout::FanOutTask`, except the number and
/// identity of children isn't fixed until the task actually runs. This is
/// what LangGraph's `Send` API covers in Python: "run this step once per
/// item in a list I only know at runtime" (once per retrieved document, once
/// per subtask an LLM just planned out), which a construction-time `Vec` of
/// children can't express.
pub struct DynamicMapTask<F> {
    id: String,
    children_fn: F,
    prefix: Option<String>,
    next_action: NextAction,
}

impl<F> DynamicMapTask<F>
where
    F: Fn(&Context) -> Vec<Arc<dyn Task>> + Send + Sync,
{
    /// `children_fn` inspects `context` and returns the child tasks to run
    /// concurrently this time — typically one per item in a list `context`
    /// already holds.
    pub fn new(id: impl Into<String>, children_fn: F) -> Self {
        Self {
            id: id.into(),
            children_fn,
            prefix: None,
            next_action: NextAction::Continue,
        }
    }

    /// Store aggregated child results under `<prefix>.<child_id>.response`
    /// instead of the default `map.<child_id>.response`.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn with_next_action(mut self, next_action: NextAction) -> Self {
        self.next_action = next_action;
        self
    }
}

#[async_trait::async_trait]
impl<F> Task for DynamicMapTask<F>
where
    F: Fn(&Context) -> Vec<Arc<dyn Task>> + Send + Sync,
{
    fn id(&self) -> &str {
        &self.id
    }

    async fn run(&self, context: Context) -> Result<TaskResult> {
        let children = (self.children_fn)(&context);
        let mut set = tokio::task::JoinSet::new();

        for child in children {
            let ctx = context.clone();
            set.spawn(async move {
                let child_id = child.id().to_string();
                (child_id, child.run(ctx).await)
            });
        }

        let mut first_error = None;
        let mut completed = 0usize;

        while let Some(joined) = set.join_next().await {
            match joined {
                Err(join_err) => {
                    first_error.get_or_insert_with(|| {
                        GraphError::TaskExecutionFailed(format!(
                            "DynamicMapTask child join error: {join_err}"
                        ))
                    });
                }
                Ok((child_id, Err(e))) => {
                    first_error.get_or_insert_with(|| {
                        GraphError::TaskExecutionFailed(format!(
                            "DynamicMapTask child '{child_id}' failed: {e}"
                        ))
                    });
                }
                Ok((child_id, Ok(result))) => {
                    if let Some(response) = result.response {
                        context.set(map_key(&self.prefix, &child_id, "response"), response)?;
                    }
                    completed += 1;
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        let summary = format!(
            "DynamicMapTask '{}' mapped over {completed} item(s)",
            self.id
        );
        Ok(TaskResult::new(Some(summary), self.next_action.clone()))
    }
}

/// Runs the same [`Task`] `runs` times concurrently and reduces the
/// responses into one — self-consistency prompting: sample an LLM call
/// several times (usually with temperature > 0) and combine the answers
/// instead of trusting a single draw.
pub struct EnsembleTask<T, R> {
    id: String,
    inner: Arc<T>,
    runs: usize,
    reducer: R,
    next_action: NextAction,
}

impl<T, R> EnsembleTask<T, R>
where
    T: Task + 'static,
    R: Fn(Vec<String>) -> String + Send + Sync,
{
    /// Run `inner` `runs` times (at least 1) and combine the responses with `reducer`.
    pub fn new(id: impl Into<String>, inner: T, runs: usize, reducer: R) -> Self {
        Self {
            id: id.into(),
            inner: Arc::new(inner),
            runs: runs.max(1),
            reducer,
            next_action: NextAction::Continue,
        }
    }

    pub fn with_next_action(mut self, next_action: NextAction) -> Self {
        self.next_action = next_action;
        self
    }
}

#[async_trait::async_trait]
impl<T, R> Task for EnsembleTask<T, R>
where
    T: Task + 'static,
    R: Fn(Vec<String>) -> String + Send + Sync,
{
    fn id(&self) -> &str {
        &self.id
    }

    async fn run(&self, context: Context) -> Result<TaskResult> {
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..self.runs {
            let inner = self.inner.clone();
            let ctx = context.clone();
            set.spawn(async move { inner.run(ctx).await });
        }

        let mut responses = Vec::with_capacity(self.runs);
        let mut first_error = None;

        while let Some(joined) = set.join_next().await {
            match joined {
                Err(join_err) => {
                    first_error.get_or_insert_with(|| {
                        GraphError::TaskExecutionFailed(format!(
                            "EnsembleTask run join error: {join_err}"
                        ))
                    });
                }
                Ok(Err(e)) => {
                    first_error.get_or_insert_with(|| {
                        GraphError::TaskExecutionFailed(format!("EnsembleTask run failed: {e}"))
                    });
                }
                Ok(Ok(result)) => {
                    if let Some(response) = result.response {
                        responses.push(response);
                    }
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        let combined = (self.reducer)(responses);
        Ok(TaskResult::new(Some(combined), self.next_action.clone()))
    }
}

/// A ready-made [`EnsembleTask`] reducer: the most common response wins,
/// ties broken by whichever came first. Fits self-consistency prompting,
/// where every run should converge on the same discrete answer.
pub fn majority_vote(responses: Vec<String>) -> String {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for response in responses {
        match counts.iter_mut().find(|(r, _)| *r == response) {
            Some(entry) => entry.1 += 1,
            None => counts.push((response, 1)),
        }
    }
    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(response, _)| response)
        .unwrap_or_default()
}
