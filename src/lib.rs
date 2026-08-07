//! Token-level streaming for [`graph-flow`](https://docs.rs/graph-flow) agent graphs.
//!
//! `graph_flow::Task::run` resolves to a single [`TaskResult`] once a task is
//! completely done — there is no way for a task to hand out partial output
//! (e.g. LLM tokens as they arrive) while it is still running. That is fine
//! for control flow, but it means anything built on `graph-flow` cannot
//! stream a response to a user the way LangGraph's `astream_events` can.
//!
//! `graphflow-stream` adds a parallel [`StreamingTask`] trait: it forwards
//! incremental [`StreamEvent`]s over a channel *while the task is still
//! running*, but still returns the same [`TaskResult`] your `NextAction`
//! control flow (`Continue` / `GoTo` / `WaitForInput` / `End`) already
//! depends on. Nothing about `graph-flow`'s executor, storage, or graph
//! building changes — this crate is additive.

use async_trait::async_trait;
use graph_flow::{error::Result, Context, Task, TaskResult};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Sending half of a stream channel, held by a running [`StreamingTask`].
pub type StreamSender = mpsc::Sender<StreamEvent>;

/// Receiving half of a stream channel, held by the caller consuming events.
pub type StreamReceiver = mpsc::Receiver<StreamEvent>;

/// An incremental event emitted by a [`StreamingTask`] while it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    /// Emitted once, before a task starts doing work.
    TaskStarted { task_id: String },
    /// A partial chunk of output (e.g. one LLM token or a few characters).
    Token { task_id: String, delta: String },
    /// Emitted once a task has produced its final [`TaskResult`].
    TaskFinished { task_id: String },
    /// Emitted if a task returns an error instead of finishing normally.
    TaskFailed { task_id: String, error: String },
}

/// A [`Task`] that can additionally stream partial output as it runs.
///
/// Implement this instead of (or in addition to) [`Task`] for any task that
/// wraps a streaming LLM call. The final [`TaskResult`] you return still
/// drives `graph-flow`'s control flow exactly as it does today; `tx` is a
/// side channel purely for incremental output.
#[async_trait]
pub trait StreamingTask: Task {
    async fn run_streaming(&self, context: Context, tx: StreamSender) -> Result<TaskResult>;
}

/// Wraps any existing [`Task`] so it also satisfies [`StreamingTask`],
/// emitting only `TaskStarted`/`TaskFinished`/`TaskFailed` around a single
/// non-streaming [`Task::run`] call. Use this to adopt `graphflow-stream`
/// incrementally: wrap the tasks you haven't converted yet so a graph can
/// mix streaming and non-streaming tasks.
pub struct NonStreaming<T>(pub T);

#[async_trait]
impl<T: Task> Task for NonStreaming<T> {
    fn id(&self) -> &str {
        self.0.id()
    }

    async fn run(&self, context: Context) -> Result<TaskResult> {
        self.0.run(context).await
    }
}

#[async_trait]
impl<T: Task> StreamingTask for NonStreaming<T> {
    async fn run_streaming(&self, context: Context, tx: StreamSender) -> Result<TaskResult> {
        let task_id = self.0.id().to_string();
        let _ = tx
            .send(StreamEvent::TaskStarted {
                task_id: task_id.clone(),
            })
            .await;

        let result = self.0.run(context).await;

        let event = match &result {
            Ok(_) => StreamEvent::TaskFinished {
                task_id: task_id.clone(),
            },
            Err(err) => StreamEvent::TaskFailed {
                task_id: task_id.clone(),
                error: err.to_string(),
            },
        };
        let _ = tx.send(event).await;

        result
    }
}

/// Runs a [`StreamingTask`] on a fresh channel, returning the receiving end
/// immediately and a [`JoinHandle`] resolving to the task's final
/// [`TaskResult`]. This is the main entry point for driving a single
/// streaming step: read from the receiver to forward tokens to your caller
/// (e.g. over SSE or a websocket) while the task keeps running in the
/// background, then await the handle for the `NextAction` decision.
pub fn spawn_streaming_task<T>(
    task: std::sync::Arc<T>,
    context: Context,
    buffer: usize,
) -> (StreamReceiver, JoinHandle<Result<TaskResult>>)
where
    T: StreamingTask + 'static,
{
    let (tx, rx) = mpsc::channel(buffer);
    let handle = tokio::spawn(async move { task.run_streaming(context, tx).await });
    (rx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_flow::{Context, NextAction, Task, TaskResult};

    struct Echo;

    #[async_trait]
    impl Task for Echo {
        fn id(&self) -> &str {
            "echo"
        }

        async fn run(&self, _context: Context) -> Result<TaskResult> {
            Ok(TaskResult::new(
                Some("hello".to_string()),
                NextAction::Continue,
            ))
        }
    }

    #[tokio::test]
    async fn non_streaming_wrapper_emits_lifecycle_events_and_preserves_result() {
        let task = std::sync::Arc::new(NonStreaming(Echo));
        let (mut rx, handle) = spawn_streaming_task(task, Context::new(), 8);

        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result.response, Some("hello".to_string()));
        assert_eq!(result.next_action, NextAction::Continue);
        assert_eq!(
            events,
            vec![
                StreamEvent::TaskStarted {
                    task_id: "echo".to_string()
                },
                StreamEvent::TaskFinished {
                    task_id: "echo".to_string()
                },
            ]
        );
    }

    #[async_trait]
    impl StreamingTask for Echo {
        async fn run_streaming(&self, _context: Context, tx: StreamSender) -> Result<TaskResult> {
            for word in ["hel", "lo"] {
                let _ = tx
                    .send(StreamEvent::Token {
                        task_id: self.id().to_string(),
                        delta: word.to_string(),
                    })
                    .await;
            }
            Ok(TaskResult::new(
                Some("hello".to_string()),
                NextAction::Continue,
            ))
        }
    }

    #[tokio::test]
    async fn streaming_task_forwards_tokens_before_finishing() {
        let task = std::sync::Arc::new(Echo);
        let (mut rx, handle) = spawn_streaming_task(task, Context::new(), 8);

        let mut deltas = Vec::new();
        while let Some(event) = rx.recv().await {
            if let StreamEvent::Token { delta, .. } = event {
                deltas.push(delta);
            }
        }

        let result = handle.await.unwrap().unwrap();
        assert_eq!(deltas, vec!["hel".to_string(), "lo".to_string()]);
        assert_eq!(result.response, Some("hello".to_string()));
    }
}
