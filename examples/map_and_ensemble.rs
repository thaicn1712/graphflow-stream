//! Two orchestration patterns Python's LangGraph has and graph-flow didn't:
//! mapping over a list whose size is only known at runtime, and running the
//! same task several times to vote on an answer.

use async_trait::async_trait;
use graph_flow::{Context, NextAction, Task, TaskResult, error::Result};
use graphflow_stream::{DynamicMapTask, EnsembleTask, majority_vote};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct SummarizeDoc {
    doc_id: String,
}

#[async_trait]
impl Task for SummarizeDoc {
    fn id(&self) -> &str {
        &self.doc_id
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        Ok(TaskResult::new(
            Some(format!("summary of {}", self.doc_id)),
            NextAction::Continue,
        ))
    }
}

struct ClassifyIntent {
    // simulates an LLM call that gives a slightly different answer each draw
    draw: Arc<AtomicUsize>,
}

#[async_trait]
impl Task for ClassifyIntent {
    fn id(&self) -> &str {
        "classify_intent"
    }

    async fn run(&self, _context: Context) -> Result<TaskResult> {
        let answers = ["refund", "refund", "cancel"];
        let i = self.draw.fetch_add(1, Ordering::SeqCst);
        Ok(TaskResult::new(
            Some(answers[i % answers.len()].to_string()),
            NextAction::Continue,
        ))
    }
}

#[tokio::main]
async fn main() {
    // however many docs a retrieval step found this run, not fixed ahead of time
    let context = Context::new();
    context
        .set(
            "retrieved_docs",
            vec![
                "doc_1".to_string(),
                "doc_2".to_string(),
                "doc_3".to_string(),
            ],
        )
        .unwrap();

    let map_task = DynamicMapTask::new("summarize_retrieved", |ctx: &Context| {
        let docs: Vec<String> = ctx.get("retrieved_docs").unwrap_or_default();
        docs.into_iter()
            .map(|doc_id| Arc::new(SummarizeDoc { doc_id }) as Arc<dyn Task>)
            .collect()
    })
    .with_prefix("summaries");

    map_task.run(context.clone()).await.unwrap();
    for doc_id in ["doc_1", "doc_2", "doc_3"] {
        let summary: Option<String> = context.get(&format!("summaries.{doc_id}.response"));
        println!("{doc_id}: {summary:?}");
    }

    let ensemble = EnsembleTask::new(
        "classify_intent_ensemble",
        ClassifyIntent {
            draw: Arc::new(AtomicUsize::new(0)),
        },
        3,
        majority_vote,
    );
    let result = ensemble.run(Context::new()).await.unwrap();
    println!("majority vote over 3 draws: {:?}", result.response);
}
