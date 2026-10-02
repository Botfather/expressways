use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use expressways_protocol::{AgentCard, AgentQuery, RegistryEvent, RegistryEventKind};
use thiserror::Error;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryWatchBatch {
    pub events: Vec<RegistryEvent>,
    pub cursor: u64,
    pub timed_out: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryEventError {
    #[error(
        "watch cursor `{provided}` is older than earliest available cursor `{earliest}`; resync with list-agents"
    )]
    CursorExpired {
        provided: u64,
        earliest: u64,
        current: u64,
    },
    #[error("registry event sequence space is exhausted")]
    SequenceExhausted,
}

#[derive(Debug, Clone)]
pub struct RegistryEventHub {
    inner: Arc<RegistryEventHubInner>,
}

#[derive(Debug)]
struct RegistryEventHubInner {
    state: Mutex<RegistryEventState>,
    notify: Notify,
    history_limit: usize,
}

#[derive(Debug)]
struct RegistryEventState {
    next_sequence: Option<u64>,
    events: VecDeque<RegistryEvent>,
}

impl RegistryEventHub {
    pub fn new(history_limit: usize) -> Self {
        Self {
            inner: Arc::new(RegistryEventHubInner {
                state: Mutex::new(RegistryEventState {
                    next_sequence: Some(1),
                    events: VecDeque::new(),
                }),
                notify: Notify::new(),
                history_limit: history_limit.max(1),
            }),
        }
    }

    pub async fn resolve_cursor(&self, cursor: Option<u64>) -> Result<u64, RegistryEventError> {
        let state = self.inner.state.lock().await;
        let effective_cursor = cursor.unwrap_or_else(|| current_cursor(&state));
        validate_cursor(&state, effective_cursor)?;
        Ok(effective_cursor)
    }

    pub async fn current_cursor(&self) -> u64 {
        let state = self.inner.state.lock().await;
        current_cursor(&state)
    }

    pub async fn record(
        &self,
        kind: RegistryEventKind,
        card: AgentCard,
    ) -> Result<RegistryEvent, RegistryEventError> {
        let mut state = self.inner.state.lock().await;
        let sequence = state
            .next_sequence
            .ok_or(RegistryEventError::SequenceExhausted)?;
        let event = RegistryEvent {
            sequence,
            timestamp: Utc::now(),
            kind,
            card,
        };
        state.next_sequence = sequence.checked_add(1);
        state.events.push_back(event.clone());
        while state.events.len() > self.inner.history_limit {
            state.events.pop_front();
        }
        drop(state);
        self.inner.notify.notify_waiters();
        Ok(event)
    }

    pub async fn watch(
        &self,
        query: &AgentQuery,
        cursor: Option<u64>,
        max_events: usize,
        wait_timeout_ms: u64,
    ) -> Result<RegistryWatchBatch, RegistryEventError> {
        let requested_max = max_events.clamp(1, 500);
        let wait_timeout = Duration::from_millis(wait_timeout_ms);
        let deadline = Instant::now() + wait_timeout;
        let effective_cursor = match cursor {
            Some(cursor) => cursor,
            None => self.current_cursor().await,
        };

        loop {
            if let Some(batch) = self
                .collect_batch(query, effective_cursor, requested_max, wait_timeout_ms == 0)
                .await?
            {
                return Ok(batch);
            }

            let notified = self.inner.notify.notified();
            match deadline.checked_duration_since(Instant::now()) {
                Some(remaining) if !remaining.is_zero() => {
                    if tokio::time::timeout(remaining, notified).await.is_ok() {
                        continue;
                    }
                }
                _ => {}
            }

            return Ok(RegistryWatchBatch {
                events: Vec::new(),
                cursor: self.current_cursor().await,
                timed_out: true,
            });
        }
    }

    async fn collect_batch(
        &self,
        query: &AgentQuery,
        cursor: u64,
        max_events: usize,
        immediate_if_empty: bool,
    ) -> Result<Option<RegistryWatchBatch>, RegistryEventError> {
        let state = self.inner.state.lock().await;
        validate_cursor(&state, cursor)?;
        let cursor_now = current_cursor(&state);
        let mut events = Vec::new();
        let mut response_cursor = cursor;
        for event in state.events.iter().filter(|event| event.sequence > cursor) {
            response_cursor = event.sequence;
            if matches_query(&event.card, query) {
                events.push(event.clone());
                if events.len() == max_events {
                    break;
                }
            }
        }

        if events.is_empty() && !immediate_if_empty {
            return Ok(None);
        }

        Ok(Some(RegistryWatchBatch {
            events,
            cursor: if response_cursor == cursor {
                cursor_now
            } else {
                response_cursor
            },
            timed_out: false,
        }))
    }
}

fn current_cursor(state: &RegistryEventState) -> u64 {
    state
        .next_sequence
        .map(|next| next.saturating_sub(1))
        .unwrap_or(u64::MAX)
}

fn earliest_available_cursor(state: &RegistryEventState) -> u64 {
    state
        .events
        .front()
        .map(|event| event.sequence.saturating_sub(1))
        .unwrap_or_else(|| current_cursor(state))
}

fn validate_cursor(state: &RegistryEventState, provided: u64) -> Result<(), RegistryEventError> {
    if state.events.is_empty() {
        return Ok(());
    }

    let earliest = earliest_available_cursor(state);
    if provided < earliest {
        return Err(RegistryEventError::CursorExpired {
            provided,
            earliest,
            current: current_cursor(state),
        });
    }

    Ok(())
}

fn matches_query(agent: &AgentCard, query: &AgentQuery) -> bool {
    if !query.include_stale && agent.expires_at <= Utc::now() {
        return false;
    }

    matches_optional(&query.principal, &agent.principal)
        && matches_skill(query.skill.as_deref(), &agent.skills)
        && matches_topic(
            query.topic.as_deref(),
            &agent.subscriptions,
            &agent.publications,
        )
}

fn matches_optional(expected: &Option<String>, actual: &str) -> bool {
    match expected {
        Some(expected) => expected == actual,
        None => true,
    }
}

fn matches_skill(expected: Option<&str>, skills: &[String]) -> bool {
    match expected {
        Some(expected) => skills
            .iter()
            .any(|skill| skill.eq_ignore_ascii_case(expected)),
        None => true,
    }
}

fn matches_topic(
    expected: Option<&str>,
    subscriptions: &[String],
    publications: &[String],
) -> bool {
    match expected {
        Some(expected) => {
            subscriptions.iter().any(|topic| topic == expected)
                || publications.iter().any(|topic| topic == expected)
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use expressways_protocol::{
        AgentCard, AgentEndpoint, AgentQuery, Classification, RegistryEventKind, RetentionClass,
    };

    use super::*;

    fn card(agent_id: &str) -> AgentCard {
        let now = Utc::now();
        AgentCard {
            agent_id: agent_id.to_owned(),
            principal: "local:agent-alpha".to_owned(),
            display_name: "Alpha".to_owned(),
            version: "1.0.0".to_owned(),
            summary: "test card".to_owned(),
            skills: vec!["summarize".to_owned()],
            subscriptions: vec!["topic:tasks".to_owned()],
            publications: vec!["topic:results".to_owned()],
            schemas: Vec::new(),
            endpoint: AgentEndpoint {
                transport: "control_tcp".to_owned(),
                address: "127.0.0.1:9000".to_owned(),
            },
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            ttl_seconds: 60,
            updated_at: now,
            last_seen_at: now,
            expires_at: now + Duration::seconds(60),
        }
    }

    #[tokio::test]
    async fn watch_returns_events_after_cursor() {
        let hub = RegistryEventHub::new(16);
        let first = hub
            .record(RegistryEventKind::Registered, card("alpha"))
            .await
            .expect("record first event");
        let second = hub
            .record(RegistryEventKind::Heartbeated, card("alpha"))
            .await
            .expect("record second event");

        let batch = hub
            .watch(&AgentQuery::default(), Some(first.sequence), 10, 0)
            .await
            .expect("watch");

        assert_eq!(batch.cursor, second.sequence);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].kind, RegistryEventKind::Heartbeated);
    }

    #[tokio::test]
    async fn expired_cursor_is_rejected_when_history_rolls() {
        let hub = RegistryEventHub::new(1);
        let first = hub
            .record(RegistryEventKind::Registered, card("alpha"))
            .await
            .expect("record first event");
        let _second = hub
            .record(RegistryEventKind::Registered, card("beta"))
            .await
            .expect("record second event");

        let error = hub
            .watch(
                &AgentQuery::default(),
                Some(first.sequence.saturating_sub(1)),
                10,
                0,
            )
            .await
            .expect_err("cursor should expire");

        assert!(matches!(error, RegistryEventError::CursorExpired { .. }));
    }

    #[tokio::test]
    async fn watch_without_cursor_observes_future_events() {
        let hub = RegistryEventHub::new(16);
        let watch_hub = hub.clone();
        let watch = tokio::spawn(async move {
            watch_hub
                .watch(&AgentQuery::default(), None, 10, 2_000)
                .await
                .expect("watch")
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let event = hub
            .record(RegistryEventKind::Registered, card("alpha"))
            .await
            .expect("record event");

        let batch = watch.await.expect("join");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].sequence, event.sequence);
    }

    #[tokio::test]
    async fn paginated_watch_cursor_does_not_skip_unreturned_events() {
        let hub = RegistryEventHub::new(16);
        let first = hub
            .record(RegistryEventKind::Registered, card("alpha"))
            .await
            .expect("record first event");
        let second = hub
            .record(RegistryEventKind::Registered, card("beta"))
            .await
            .expect("record second event");
        let third = hub
            .record(RegistryEventKind::Registered, card("gamma"))
            .await
            .expect("record third event");

        let first_page = hub
            .watch(&AgentQuery::default(), Some(0), 1, 0)
            .await
            .expect("first page");
        assert_eq!(first_page.events, vec![first]);
        assert_eq!(first_page.cursor, first_page.events[0].sequence);

        let second_page = hub
            .watch(&AgentQuery::default(), Some(first_page.cursor), 1, 0)
            .await
            .expect("second page");
        assert_eq!(second_page.events, vec![second]);

        let third_page = hub
            .watch(&AgentQuery::default(), Some(second_page.cursor), 1, 0)
            .await
            .expect("third page");
        assert_eq!(third_page.events, vec![third]);
    }

    #[tokio::test]
    async fn sequence_exhaustion_never_wraps_or_duplicates_cursors() {
        let hub = RegistryEventHub::new(16);
        hub.inner.state.lock().await.next_sequence = Some(u64::MAX);

        let last = hub
            .record(RegistryEventKind::Registered, card("last"))
            .await
            .expect("record final sequence");
        assert_eq!(last.sequence, u64::MAX);
        assert_eq!(hub.current_cursor().await, u64::MAX);

        let error = hub
            .record(RegistryEventKind::Registered, card("overflow"))
            .await
            .expect_err("exhausted sequence must fail");
        assert_eq!(error, RegistryEventError::SequenceExhausted);
        assert_eq!(hub.current_cursor().await, u64::MAX);
    }
}
