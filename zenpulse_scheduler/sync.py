import logging
from .models import ScheduleConfig
from .registry import JobRegistry
from .triggers import build_trigger

logger = logging.getLogger(__name__)

def _auto_create_configs():
    """
    Auto-create ScheduleConfig entries for registered jobs that
    don't yet have a database entry. Uses defaults from the decorator.
    """
    registered_jobs = JobRegistry.get_all_entries()
    if not registered_jobs:
        return

    existing_keys = set(
        ScheduleConfig.objects.values_list('job_key', flat=True)
    )

    for job_key, entry in registered_jobs.items():
        if job_key in existing_keys:
            continue

        defaults = entry.get('defaults', {})

        config_data = {
            'job_key': job_key,
            'enabled': defaults.get('enabled', True),
            'trigger_type': defaults.get('trigger', 'interval'),
            'interval_value': defaults.get('interval_value', 5),
            'interval_unit': defaults.get('interval_unit', 'minutes'),
            'cron_minute': defaults.get('cron_minute', '*'),
            'cron_hour': defaults.get('cron_hour', '*'),
            'cron_day': defaults.get('cron_day', '*'),
            'cron_month': defaults.get('cron_month', '*'),
            'cron_day_of_week': defaults.get('cron_day_of_week', '*'),
            'max_instances': defaults.get('max_instances', 1),
            'coalesce': defaults.get('coalesce', True),
            'misfire_grace_time': defaults.get('misfire_grace_time', 60),
            'log_policy': defaults.get('log_policy', 'failures'),
        }

        try:
            ScheduleConfig.objects.create(**config_data)
            trigger_info = config_data['trigger_type']
            if trigger_info == 'interval':
                trigger_info += f" ({config_data['interval_value']} {config_data['interval_unit']})"
            logger.info(
                f"Auto-created ScheduleConfig for job '{job_key}' "
                f"[{trigger_info}, enabled={config_data['enabled']}]"
            )
        except Exception as e:
            logger.error(f"Failed to auto-create config for job '{job_key}': {e}")


def sync_jobs(scheduler, last_synced_data):
    """
    Reconciles the DB ScheduleConfig with the in-memory APScheduler.
    last_synced_data: dict {job_key: (enabled, updated_at_timestamp)}
    """
    logger.debug("Starting sync_jobs...")

    # Step 0: Auto-create DB configs for any new registered jobs
    _auto_create_configs()

    configs = ScheduleConfig.objects.all()
    
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
    for job in scheduler.get_jobs():
        if job.id not in active_db_jobs:
            logger.info(f"Job {job.id} not in DB config. Removing.")
            scheduler.remove_job(job.id)
