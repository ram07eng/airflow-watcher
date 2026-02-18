"""Prometheus Exporter - Exposes metrics in Prometheus format."""

import logging
from typing import Optional, Dict, Any, List
from flask import Blueprint, Response

logger = logging.getLogger(__name__)


class PrometheusExporter:
    """Exports metrics in Prometheus format via HTTP endpoint."""
    
    def __init__(self, prefix: str = "airflow_watcher"):
        """Initialize Prometheus exporter.
        
        Args:
            prefix: Metric name prefix
        """
        self.prefix = prefix
        self._metrics_cache: Dict[str, Any] = {}
        self._labels: Dict[str, str] = {}
    
    def set_labels(self, labels: Dict[str, str]):
        """Set default labels for all metrics.
        
        Args:
            labels: Label key-value pairs
        """
        self._labels = labels
    
    def _format_labels(self, extra_labels: Optional[Dict[str, str]] = None) -> str:
        """Format labels for Prometheus format."""
        all_labels = {**self._labels, **(extra_labels or {})}
        if not all_labels:
            return ""
        label_str = ",".join(f'{k}="{v}"' for k, v in all_labels.items())
        return f"{{{label_str}}}"
    
    def _format_metric_name(self, name: str) -> str:
        """Format metric name for Prometheus (replace . with _)."""
        return f"{self.prefix}_{name.replace('.', '_')}"
    
    def generate_metrics(self) -> str:
        """Generate Prometheus-formatted metrics string.
        
        Returns:
            Prometheus exposition format string
        """
        from airflow_watcher.metrics.collector import MetricsCollector
        
        collector = MetricsCollector()
        metrics = collector.collect()
        
        lines = []
        
        # Add HELP and TYPE comments for each metric
        metric_info = {
            "failures.total_24h": ("gauge", "Total DAG failures in last 24 hours"),
            "failures.unique_dags_24h": ("gauge", "Unique DAGs that failed in last 24 hours"),
            "failures.rate_percent": ("gauge", "DAG failure rate percentage"),
            "sla.misses_24h": ("gauge", "SLA misses in last 24 hours"),
            "sla.miss_rate_percent": ("gauge", "SLA miss rate percentage"),
            "tasks.failed_24h": ("gauge", "Failed tasks in last 24 hours"),
            "tasks.retry_24h": ("gauge", "Tasks pending retry in last 24 hours"),
            "tasks.long_running": ("gauge", "Currently long-running tasks"),
            "scheduling.missed_24h": ("gauge", "Missed schedules in last 24 hours"),
            "scheduling.delayed_dags": ("gauge", "Currently delayed DAGs"),
            "dags.unhealthy": ("gauge", "Number of unhealthy DAGs"),
            "dags.total": ("gauge", "Total number of DAGs"),
            "dags.healthy_percent": ("gauge", "Percentage of healthy DAGs"),
            "dependencies.failed_sensors": ("gauge", "Failed sensor tasks"),
            "dependencies.blocked_dags": ("gauge", "DAGs blocked by dependencies"),
        }
        
        labels = self._format_labels()
        
        for name, value in metrics.to_dict().items():
            if not isinstance(value, (int, float)):
                continue
                
            metric_name = self._format_metric_name(name)
            metric_type, help_text = metric_info.get(name, ("gauge", ""))
            
            lines.append(f"# HELP {metric_name} {help_text}")
            lines.append(f"# TYPE {metric_name} {metric_type}")
            lines.append(f"{metric_name}{labels} {value}")
        
        # Add collection timestamp
        timestamp_name = self._format_metric_name("collection_timestamp")
        lines.append(f"# HELP {timestamp_name} Timestamp of last metrics collection")
        lines.append(f"# TYPE {timestamp_name} gauge")
        lines.append(f"{timestamp_name}{labels} {metrics.collected_at.timestamp()}")
        
        return "\n".join(lines) + "\n"
    
    def create_blueprint(self) -> Blueprint:
        """Create Flask blueprint for /metrics endpoint.
        
        Returns:
            Flask Blueprint with /metrics route
        """
        bp = Blueprint("watcher_prometheus", __name__)
        
        @bp.route("/metrics")
        def metrics_endpoint():
            """Prometheus metrics endpoint."""
            try:
                metrics_text = self.generate_metrics()
                return Response(
                    metrics_text,
                    mimetype="text/plain; version=0.0.4; charset=utf-8",
                )
            except Exception as e:
                logger.error(f"Error generating Prometheus metrics: {e}")
                return Response(
                    f"# Error generating metrics: {e}\n",
                    status=500,
                    mimetype="text/plain",
                )
        
        return bp
    
    @classmethod
    def from_config(cls, config: "WatcherConfig") -> "PrometheusExporter":
        """Create exporter from WatcherConfig.
        
        Args:
            config: WatcherConfig instance
            
        Returns:
            Configured PrometheusExporter
        """
        exporter = cls(prefix=config.prometheus_prefix)
        if config.prometheus_labels:
            exporter.set_labels(config.prometheus_labels)
        return exporter
