import logging
from .models import ScheduleConfig
from .registry import JobRegistry
from .triggers import build_trigger

logger = logging.getLogger(__name__)

def sync_jobs(scheduler, last_synced_data):
    """
    Reconciles the DB ScheduleConfig with the in-memory APScheduler.
    last_synced_data: dict {job_key: (enabled, updated_at_timestamp)}
    """
    logger.debug("Starting sync_jobs...")
    # print("DEBUG: Syncing jobs...") 
    configs = ScheduleConfig.objects.all()
    # print(f"DEBUG: Found {len(configs)} configs in DB.")
    
    # Track which jobs we've seen in the DB to handle removals
    active_db_jobs = set()

    for config in configs:
        job_key = config.job_key
        active_db_jobs.add(job_key)
        
        # Check cache to see if update is needed
        current_state = (config.enabled, config.updated_at.timestamp())
        if job_key in last_synced_data and last_synced_data[job_key] == current_state:
            # No changes, skip
            continue
            
        last_synced_data[job_key] = current_state

        # 1. Check if job is in registry
        func = JobRegistry.get_job(job_key)
        if not func:
            logger.warning(f"Job '{job_key}' found in config but NOT in registry. Skipping.")
            continue
        
        # 2. Check if job exists in scheduler
        existing_job = scheduler.get_job(job_key)
        
        # 3. Handle Enabled/Disabled
        if not config.enabled:
            # If exists, remove it
            if existing_job:
                logger.info(f"Removing disabled job: {job_key}")
                scheduler.remove_job(job_key)
            continue
        
        # 4. Handle Active Jobs
        trigger = build_trigger(config)
        
        kwargs = {
            'id': job_key,
            'name': job_key,
            'func': func,
            'trigger': trigger,
            'replace_existing': True,
            'coalesce': config.coalesce,
            'max_instances': config.max_instances,
            'misfire_grace_time': config.misfire_grace_time,
        }

        # Update or Add
        logger.info(f"Syncing job: {job_key}")
        try:
            scheduler.add_job(**kwargs)
        except Exception as e:
            logger.error(f"Failed to add/update job {job_key}: {e}")

    # 5. Remove jobs that are in Scheduler but NOT in Config (or deleted from DB)
    # Be careful not to remove internal scheduler jobs if any (usually none in MemoryJobStore unless added manually)
    for job in scheduler.get_jobs():
        if job.id not in active_db_jobs:
            logger.info(f"Job {job.id} not in DB config. Removing.")
            scheduler.remove_job(job.id)
