use std::sync::Mutex;
use std::time::{Duration, Instant};

use expressways_audit::AuditLogSummary;
use expressways_protocol::{
    Action, AdopterStatusView, AuditMetricsView, BrokerMetricsView, OperationMetricsView,
    ResilienceMetricsView, StorageMetricsView, StreamMetricsView,
};
use expressways_storage::StorageStats;

#[derive(Debug)]
pub struct MetricsCollector {
    started_at: Instant,
    state: Mutex<MetricsState>,
}

#[derive(Debug, Default)]
struct MetricsState {
    total_requests: u64,
    health_requests: u64,
    admin_requests: u64,
    auth_failures: u64,
    policy_denials: u64,
    quota_denials: u64,
    storage_failures: u64,
    audit_failures: u64,
    publish: OperationStats,
    consume: OperationStats,
    stream_watch: OperationStats,
    open_streams: u64,
    opened_streams: u64,
    closed_streams: u64,
    keepalives_sent: u64,
    event_frames_sent: u64,
    events_delivered: u64,
    delivery_failures: u64,
    slow_consumer_drops: u64,
    idle_timeouts: u64,
}

#[derive(Debug, Default)]
struct OperationStats {
    requests: u64,
    successes: u64,
    failures: u64,
    total_latency_ms: u128,
    max_latency_ms: u64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            state: Mutex::new(MetricsState::default()),
        }
    }

    pub fn record_request(&self, action: &Action) {
        let mut state = self.state.lock().expect("metrics lock");
        state.total_requests += 1;
        match action {
            Action::Health => state.health_requests += 1,
            Action::Admin => state.admin_requests += 1,
            Action::Publish => state.publish.requests += 1,
            Action::Consume => state.consume.requests += 1,
        }
    }

    pub fn record_auth_failure(&self) {
        self.state.lock().expect("metrics lock").auth_failures += 1;
    }

    pub fn record_policy_denial(&self) {
        self.state.lock().expect("metrics lock").policy_denials += 1;
    }

    pub fn record_quota_denial(&self) {
        self.state.lock().expect("metrics lock").quota_denials += 1;
    }

    pub fn record_storage_failure(&self) {
        self.state.lock().expect("metrics lock").storage_failures += 1;
    }

    pub fn record_audit_failure(&self) {
        self.state.lock().expect("metrics lock").audit_failures += 1;
    }

    pub fn record_publish_result(&self, success: bool, latency: Duration) {
        let mut state = self.state.lock().expect("metrics lock");
        update_operation(&mut state.publish, success, latency);
    }

    pub fn record_consume_result(&self, success: bool, latency: Duration) {
        let mut state = self.state.lock().expect("metrics lock");
        update_operation(&mut state.consume, success, latency);
    }

    pub fn record_stream_opened(&self) {
        let mut state = self.state.lock().expect("metrics lock");
        state.open_streams += 1;
        state.opened_streams += 1;
        state.stream_watch.requests += 1;
    }

    pub fn record_stream_closed(&self) {
        let mut state = self.state.lock().expect("metrics lock");
        state.open_streams = state.open_streams.saturating_sub(1);
        state.closed_streams += 1;
    }

    pub fn record_stream_keepalive(&self) {
        self.state.lock().expect("metrics lock").keepalives_sent += 1;
    }

    pub fn record_stream_delivery(&self, delivered_events: usize, latency: Duration) {
        let mut state = self.state.lock().expect("metrics lock");
        state.event_frames_sent += 1;
        state.events_delivered += delivered_events as u64;
        update_operation(&mut state.stream_watch, true, latency);
    }

    pub fn record_stream_delivery_failure(&self, latency: Duration) {
        let mut state = self.state.lock().expect("metrics lock");
        state.delivery_failures += 1;
        update_operation(&mut state.stream_watch, false, latency);
    }

    pub fn record_stream_slow_consumer_drop(&self) {
        self.state.lock().expect("metrics lock").slow_consumer_drops += 1;
    }

    pub fn record_stream_idle_timeout(&self) {
        self.state.lock().expect("metrics lock").idle_timeouts += 1;
    }

    pub fn snapshot(
        &self,
        storage: StorageStats,
        audit: AuditLogSummary,
        service_mode: String,
        degraded_components: Vec<String>,
        adopters: Vec<AdopterStatusView>,
    ) -> BrokerMetricsView {
        let state = self.state.lock().expect("metrics lock");
        BrokerMetricsView {
            uptime_seconds: self.started_at.elapsed().as_secs(),
            total_requests: state.total_requests,
            health_requests: state.health_requests,
            admin_requests: state.admin_requests,
            auth_failures: state.auth_failures,
            policy_denials: state.policy_denials,
            quota_denials: state.quota_denials,
            storage_failures: state.storage_failures,
            audit_failures: state.audit_failures,
            publish: operation_view(&state.publish),
            consume: operation_view(&state.consume),
            storage: StorageMetricsView {
                topic_count: storage.topic_count,
                segment_count: storage.segment_count,
                total_bytes: storage.total_bytes,
                reclaimed_segments: storage.maintenance.reclaimed_segments,
                reclaimed_bytes: storage.maintenance.reclaimed_bytes,
                recovered_segments: storage.maintenance.recovered_segments,
                truncated_bytes: storage.maintenance.truncated_bytes,
            },
            audit: AuditMetricsView {
                event_count: audit.event_count,
                last_hash: audit.last_hash,
            },
            streams: StreamMetricsView {
                open_streams: state.open_streams,
                opened_streams: state.opened_streams,
                closed_streams: state.closed_streams,
                keepalives_sent: state.keepalives_sent,
                event_frames_sent: state.event_frames_sent,
                events_delivered: state.events_delivered,
                delivery_failures: state.delivery_failures,
                slow_consumer_drops: state.slow_consumer_drops,
                idle_timeouts: state.idle_timeouts,
                watch_stream: operation_view(&state.stream_watch),
            },
            resilience: ResilienceMetricsView {
                service_mode,
                degraded_components,
            },
            adopters,
        }
    }
}

fn update_operation(operation: &mut OperationStats, success: bool, latency: Duration) {
    let latency_ms = latency.as_millis() as u64;
    if success {
        operation.successes += 1;
    } else {
        operation.failures += 1;
    }
    operation.total_latency_ms += latency.as_millis();
    operation.max_latency_ms = operation.max_latency_ms.max(latency_ms);
}

fn operation_view(operation: &OperationStats) -> OperationMetricsView {
    let samples = operation.successes + operation.failures;
    let average_latency_ms = if samples == 0 {
        0
    } else {
        (operation.total_latency_ms / samples as u128) as u64
    };

    OperationMetricsView {
        requests: operation.requests,
        successes: operation.successes,
        failures: operation.failures,
        average_latency_ms,
        max_latency_ms: operation.max_latency_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expressways_storage::MaintenanceStats;

    #[test]
    fn metrics_snapshot_aggregates_counters() {
        let metrics = MetricsCollector::new();
        metrics.record_request(&Action::Publish);
        metrics.record_publish_result(true, Duration::from_millis(12));
        metrics.record_auth_failure();
        metrics.record_stream_opened();
        metrics.record_stream_keepalive();
        metrics.record_stream_delivery(3, Duration::from_millis(4));
        metrics.record_stream_closed();

        let snapshot = metrics.snapshot(
            StorageStats {
                topic_count: 1,
                segment_count: 2,
                total_bytes: 512,
                maintenance: MaintenanceStats::default(),
            },
            AuditLogSummary {
                event_count: 3,
                last_hash: Some("abc".to_owned()),
            },
            "ok".to_owned(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.publish.requests, 1);
        assert_eq!(snapshot.publish.successes, 1);
        assert_eq!(snapshot.publish.average_latency_ms, 12);
        assert_eq!(snapshot.auth_failures, 1);
        assert_eq!(snapshot.audit.event_count, 3);
        assert_eq!(snapshot.streams.opened_streams, 1);
        assert_eq!(snapshot.streams.closed_streams, 1);
        assert_eq!(snapshot.streams.keepalives_sent, 1);
        assert_eq!(snapshot.streams.events_delivered, 3);
        assert_eq!(snapshot.streams.watch_stream.requests, 1);
        assert_eq!(snapshot.streams.watch_stream.successes, 1);
        assert_eq!(snapshot.resilience.service_mode, "ok");
        assert!(snapshot.adopters.is_empty());
    }
}
