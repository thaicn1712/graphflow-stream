use async_trait::async_trait;
use graph_flow::{error::Result, Context, Task, TaskResult};
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

#[async_trait]
pub trait StreamingTask: Task {
    async fn run_streaming(&self, context: Context, tx: StreamSender) -> Result<TaskResult>;
}

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
