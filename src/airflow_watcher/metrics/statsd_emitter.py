"""StatsD Emitter - Sends metrics to StatsD/Datadog."""

import logging
import socket
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class StatsDEmitter:
    """Emits metrics to StatsD or DogStatsD (Datadog)."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8125,
        prefix: str = "airflow.watcher",
        use_dogstatsd: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ):
        """Initialize StatsD emitter.

        Args:
            host: StatsD server hostname
            port: StatsD server port
            prefix: Metric name prefix
            use_dogstatsd: Use DogStatsD format (Datadog)
            tags: Default tags to add to all metrics
        """
        self.host = host
        self.port = port
        self.prefix = prefix
        self.use_dogstatsd = use_dogstatsd
        self.default_tags = tags or {}
        self._socket: Optional[socket.socket] = None
        self._enabled = True

    def _get_socket(self) -> socket.socket:
        """Get or create UDP socket."""
        if self._socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return self._socket

    def _format_tags(self, tags: Optional[Dict[str, str]] = None) -> str:
        """Format tags for DogStatsD."""
        all_tags = {**self.default_tags, **(tags or {})}
        if not all_tags or not self.use_dogstatsd:
            return ""
        tag_str = ",".join(f"{k}:{v}" for k, v in all_tags.items())
        return f"|#{tag_str}"

    def _send(self, data: str) -> bool:
        """Send data to StatsD server."""
        if not self._enabled:
            return False

        try:
            sock = self._get_socket()
            sock.sendto(data.encode(), (self.host, self.port))
            return True
        except Exception as e:
            logger.warning(f"Failed to send to StatsD: {e}")
            return False

    def gauge(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Send a gauge metric.

        Args:
            name: Metric name
            value: Metric value
            tags: Optional tags (DogStatsD only)
        """
        metric_name = f"{self.prefix}.{name}"
        tag_str = self._format_tags(tags)
        data = f"{metric_name}:{value}|g{tag_str}"
        return self._send(data)

    def counter(
        self,
        name: str,
        value: int = 1,
        tags: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Send a counter metric.

        Args:
            name: Metric name
            value: Increment value (default 1)
            tags: Optional tags (DogStatsD only)
        """
        metric_name = f"{self.prefix}.{name}"
        tag_str = self._format_tags(tags)
        data = f"{metric_name}:{value}|c{tag_str}"
        return self._send(data)

    def timing(
        self,
        name: str,
        value_ms: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Send a timing metric.

        Args:
            name: Metric name
            value_ms: Duration in milliseconds
            tags: Optional tags (DogStatsD only)
        """
        metric_name = f"{self.prefix}.{name}"
        tag_str = self._format_tags(tags)
        data = f"{metric_name}:{value_ms}|ms{tag_str}"
        return self._send(data)

    def histogram(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Send a histogram metric (DogStatsD only).

        Args:
            name: Metric name
            value: Metric value
            tags: Optional tags
        """
        if not self.use_dogstatsd:
            # Fall back to timing for standard StatsD
            return self.timing(name, value, tags)

        metric_name = f"{self.prefix}.{name}"
        tag_str = self._format_tags(tags)
        data = f"{metric_name}:{value}|h{tag_str}"
        return self._send(data)

    def emit_watcher_metrics(self, metrics: "WatcherMetrics") -> Dict[str, bool]:
        """Emit all Watcher metrics.

        Args:
            metrics: WatcherMetrics object

        Returns:
            Dict of metric names to success status
        """

        results = {}
        for name, value in metrics.to_dict().items():
            if isinstance(value, (int, float)):
                results[name] = self.gauge(name, value)

        return results

    def close(self):
        """Close the socket connection."""
        if self._socket:
            self._socket.close()
            self._socket = None

    def disable(self):
        """Disable metric emission."""
        self._enabled = False

    def enable(self):
        """Enable metric emission."""
        self._enabled = True

    @classmethod
    def from_config(cls, config: "WatcherConfig") -> "StatsDEmitter":
        """Create emitter from WatcherConfig.

        Args:
            config: WatcherConfig instance

        Returns:
            Configured StatsDEmitter
        """
        return cls(
            host=config.statsd_host,
            port=config.statsd_port,
            prefix=config.statsd_prefix,
            use_dogstatsd=config.use_dogstatsd,
            tags=config.statsd_tags,
        )
