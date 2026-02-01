import logging
import traceback
import socket
import os
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from django.utils import timezone
from .models import ScheduleConfig, JobExecutionLog

logger = logging.getLogger(__name__)

def get_config_log_policy(job_id):
    """
    Helper to fetch log policy for a given job_id.
    Cache this or fetch from DB? Fetching from DB is safer for real-time updates,
    but we might want to optimize if high throughput.
    """
    try:
        # job_id in APScheduler will be the job_key from our model
        config = ScheduleConfig.objects.filter(job_key=job_id).first()
        if config:
            return config.log_policy
    except Exception:
        pass
    return 'none' # Default to none if not found

def handle_job_execution(event):
    """
    Listener for SUCCESS and ERROR events.
    """
    job_id = event.job_id
    policy = get_config_log_policy(job_id)

    if policy == 'none':
        return

    is_error = event.exception is not None

    # Logic:
    # FAILURES: Record only if is_error
    # ALL: Record everything
    if policy == 'failures' and not is_error:
        return

    # Prepare Log Entry
    status = 'fail' if is_error else 'success'
    
    # Calculate duration (APScheduler events might not have duration directly in all versions, 
    # but let's check `event.retval` or simple generic logging)
    # Actually `event` object in APScheduler has `scheduled_run_time` and loop time.
    # We can infer valid duration if we wrap jobs, but here we might rely on estimated diff logic 
    # or just record 0 if unavailable.
    # NOTE: APScheduler `JobExecutionEvent` does not strictly capture duration easily without wrapper.
    # However, we can just log the event.
    
    duration = 0.0 # Placeholder, or implement wrapper logic for precise timing later.

    exception_type = None
    exception_message = None
    tb = None

    if is_error:
        exception_type = type(event.exception).__name__
        exception_message = str(event.exception)
        try:
             tb = "".join(traceback.format_tb(event.traceback))
        except:
             tb = str(event.traceback)

    try:
        JobExecutionLog.objects.create(
            job_key=job_id,
            status=status,
            duration_ms=duration, # Update if we implement wrapper timing
            exception_type=exception_type,
            exception_message=exception_message,
            traceback=tb,
            hostname=socket.gethostname(),
            pid=os.getpid()
        )
    except Exception as e:
        logger.error(f"Failed to write execution log for job {job_id}: {e}")

