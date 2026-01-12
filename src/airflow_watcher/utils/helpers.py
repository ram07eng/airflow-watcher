"""Helper utilities for Airflow Watcher."""

from datetime import datetime, timedelta
from typing import Optional


def format_duration(seconds: Optional[float]) -> str:
    """Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Human-readable duration string
    """
    if seconds is None:
        return "N/A"
    
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_datetime(dt: Optional[datetime], format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime to string.
    
    Args:
        dt: Datetime object
        format_str: Format string
        
    Returns:
        Formatted datetime string
    """
    if dt is None:
        return "N/A"
    return dt.strftime(format_str)


def time_ago(dt: Optional[datetime]) -> str:
    """Get human-readable time ago string.
    
    Args:
        dt: Datetime object
        
    Returns:
        Human-readable "time ago" string
    """
    if dt is None:
        return "N/A"
    
    now = datetime.utcnow()
    delta = now - dt
    
    if delta < timedelta(minutes=1):
        return "just now"
    elif delta < timedelta(hours=1):
        minutes = int(delta.total_seconds() / 60)
        return f"{minutes}m ago"
    elif delta < timedelta(days=1):
        hours = int(delta.total_seconds() / 3600)
        return f"{hours}h ago"
    else:
        days = delta.days
        return f"{days}d ago"


def truncate_string(s: str, max_length: int = 100) -> str:
    """Truncate string to max length.
    
    Args:
        s: String to truncate
        max_length: Maximum length
        
    Returns:
        Truncated string with ellipsis if needed
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - 3] + "..."


def get_severity_color(failure_count: int) -> str:
    """Get severity color based on failure count.
    
    Args:
        failure_count: Number of failures
        
    Returns:
        CSS color string
    """
    if failure_count == 0:
        return "#4caf50"  # Green
    elif failure_count <= 2:
        return "#ff9800"  # Orange
    else:
        return "#f44336"  # Red


def calculate_success_rate(total: int, failed: int) -> float:
    """Calculate success rate percentage.
    
    Args:
        total: Total runs
        failed: Failed runs
        
    Returns:
        Success rate as percentage
    """
    if total == 0:
        return 100.0
    return round((total - failed) / total * 100, 2)
