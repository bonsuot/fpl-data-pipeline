import datetime

##--------------------------------------------------------------------------------------------##
log_entries = []

def log_event(event_type, message, table=None):
    """Log an event and store it in the log list."""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def get_log_entries():
    """Return all collected log entries."""
    return log_entries

def clear_logs():
    """Clear all log entries."""
    log_entries.clear()


