import time
import signal
import sys
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from django.conf import settings
from .sync import sync_jobs
from .listeners import handle_job_execution
from .locks import get_best_lock

logger = logging.getLogger(__name__)

class ZenPulseEngine:
    def __init__(self, sync_interval=10, use_lock=False):
        self.sync_interval = sync_interval
        self.use_lock = use_lock
        self.lock = get_best_lock() if use_lock else None
        self.running = False
        
        self.scheduler = BackgroundScheduler(
            jobstores={'default': MemoryJobStore()},
            executors={'default': ThreadPoolExecutor(20)},
            job_defaults={
                'coalesce': True,
                'max_instances': 1
            },
            timezone=getattr(settings, 'TIME_ZONE', 'UTC')
        )

    def start(self):
        # 1. Acquire Lock
        if self.use_lock:
            logger.info("Acquiring lock...")
            if not self.lock.acquire():
                logger.error("Could not acquire lock. Another instance is likely running. Exiting.")
                return
            logger.info("Lock acquired.")

        # 2. Setup Signal Handlers
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # 3. Setup Listeners
        self.scheduler.add_listener(handle_job_execution, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

        # 4. Start Scheduler
        self.scheduler.start()
        self.running = True
        logger.info("ZenPulse Scheduler started.")

        # Cache for simple change detection: {job_key: (enabled, updated_at_ts)}
        self.last_synced_data = {}

        # 5. Main Sync Loop
        try:
            while self.running:
                try:
                    sync_jobs(self.scheduler, self.last_synced_data)
                except Exception as e:
                    logger.error(f"Error in sync loop: {e}")
                
                # Sleep in small chunks to handle shutdown signals faster
                for _ in range(self.sync_interval):
                    if not self.running: 
                        break
                    time.sleep(1)
        except Exception as e:
            logger.critical(f"Engine crashed: {e}")
        finally:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        if not self.running: return # Already stopped
        
        logger.info("Shutting down ZenPulse Scheduler...")
        self.running = False
        self.scheduler.shutdown()
        
        if self.use_lock:
            self.lock.release()
            logger.info("Lock released.")
        
        logger.info("Shutdown complete.")
