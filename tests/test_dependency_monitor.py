"""Tests for DependencyMonitor."""

from datetime import datetime, timedelta

from airflow_watcher.monitors.dependency_monitor import DependencyMonitor


class TestDependencyMonitor:
    """Tests for DependencyMonitor."""

    def test_init(self):
        monitor = DependencyMonitor()
        assert monitor.config is not None

    def test_init_with_custom_config(self):
        from airflow_watcher.config import WatcherConfig

        config = WatcherConfig(failure_lookback_hours=6)
        monitor = DependencyMonitor(config=config)
        assert monitor.config is config

    def test_monitor_has_required_methods(self):
        """DependencyMonitor exposes expected public API."""
        monitor = DependencyMonitor()
        assert callable(getattr(monitor, "get_upstream_failures", None))
        assert callable(getattr(monitor, "get_cascading_failure_impact", None))
        assert callable(getattr(monitor, "get_cross_dag_dependencies", None))
        assert callable(getattr(monitor, "get_dependency_chain_status", None))
        assert callable(getattr(monitor, "get_failure_correlation", None))

    # ------------------------------------------------------------------
    # _get_all_downstream — pure recursive logic (no DB)
    # ------------------------------------------------------------------

    def _make_task(self, task_id, downstream_ids=None):
        """Minimal task stub for _get_all_downstream tests."""

        class _Task:
            def __init__(self, tid, ds):
                self.task_id = tid
                self._ds_tasks = ds  # list of _Task instances

            @property
            def downstream_list(self):
                return self._ds_tasks

        stubs = [_Task(tid, []) for tid in (downstream_ids or [])]
        return _Task(task_id, stubs)

    def test_get_all_downstream_no_children(self):
        monitor = DependencyMonitor()
        task = self._make_task("root")
        result = monitor._get_all_downstream(task, set())
        assert result == set()

    def test_get_all_downstream_direct_children(self):
        monitor = DependencyMonitor()
        task = self._make_task("root", downstream_ids=["child_a", "child_b"])
        result = monitor._get_all_downstream(task, set())
        assert result == {"child_a", "child_b"}

    def test_get_all_downstream_no_duplicate_visited(self):
        """Pre-visited nodes are not counted again."""
        monitor = DependencyMonitor()
        task = self._make_task("root", downstream_ids=["already_visited"])
        result = monitor._get_all_downstream(task, visited={"already_visited"})
        assert "already_visited" not in result

    # ------------------------------------------------------------------
    # Failure correlation sliding-window logic (pure, no DB)
    # ------------------------------------------------------------------

    def _run_correlation_logic(self, failed_runs, window_minutes=10):
        """Replicate the pair_map logic from get_failure_correlation."""
        pair_map = {}
        for i, run1 in enumerate(failed_runs):
            for j in range(i + 1, len(failed_runs)):
                run2 = failed_runs[j]
                time_diff = abs((run1["end_date"] - run2["end_date"]).total_seconds() / 60)
                if time_diff > window_minutes:
                    break
                if run1["dag_id"] != run2["dag_id"]:
                    pair = tuple(sorted([run1["dag_id"], run2["dag_id"]]))
                    if pair in pair_map:
                        pair_map[pair]["co_failure_count"] += 1
                    else:
                        pair_map[pair] = {"dag_1": pair[0], "dag_2": pair[1], "co_failure_count": 1}
        return pair_map

    def test_correlation_no_co_failures(self):
        """DAGs failing far apart produce no correlations."""
        now = datetime.utcnow()
        runs = [
            {"dag_id": "dag_a", "end_date": now},
            {"dag_id": "dag_b", "end_date": now + timedelta(hours=1)},
        ]
        result = self._run_correlation_logic(runs)
        assert result == {}

    def test_correlation_same_dag_ignored(self):
        """Two runs of the same DAG are not counted as a pair."""
        now = datetime.utcnow()
        runs = [
            {"dag_id": "dag_a", "end_date": now},
            {"dag_id": "dag_a", "end_date": now + timedelta(seconds=30)},
        ]
        result = self._run_correlation_logic(runs)
        assert result == {}

    def test_correlation_co_failure_counted(self):
        """DAGs failing within window are counted as a correlated pair."""
        now = datetime.utcnow()
        runs = [
            {"dag_id": "dag_a", "end_date": now},
            {"dag_id": "dag_b", "end_date": now + timedelta(minutes=5)},
        ]
        result = self._run_correlation_logic(runs)
        assert ("dag_a", "dag_b") in result
        assert result[("dag_a", "dag_b")]["co_failure_count"] == 1

    def test_correlation_multiple_co_occurrences(self):
        """Repeated co-failures increment the counter."""
        now = datetime.utcnow()
        runs = [
            {"dag_id": "dag_a", "end_date": now},
            {"dag_id": "dag_b", "end_date": now + timedelta(minutes=1)},
            {"dag_id": "dag_a", "end_date": now + timedelta(minutes=2)},
            {"dag_id": "dag_b", "end_date": now + timedelta(minutes=3)},
        ]
        result = self._run_correlation_logic(runs)
        # At least one co-occurrence for (dag_a, dag_b)
        assert result[("dag_a", "dag_b")]["co_failure_count"] >= 1

    def test_correlation_pair_key_sorted(self):
        """Pair key is always sorted (dag_a, dag_b) regardless of order."""
        now = datetime.utcnow()
        runs = [
            {"dag_id": "zzz_dag", "end_date": now},
            {"dag_id": "aaa_dag", "end_date": now + timedelta(minutes=1)},
        ]
        result = self._run_correlation_logic(runs)
        assert ("aaa_dag", "zzz_dag") in result
