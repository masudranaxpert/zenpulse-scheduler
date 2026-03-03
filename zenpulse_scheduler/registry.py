import logging

logger = logging.getLogger(__name__)

class JobRegistry:
    _registry = {}

    @classmethod
    def register(cls, name, **defaults):
        def decorator(func):
            if name in cls._registry:
                logger.warning(f"Job with key '{name}' already registered. Overwriting.")
            cls._registry[name] = {
                'func': func,
                'defaults': defaults,
            }
            return func
        return decorator

    @classmethod
    def get_job(cls, name):
        entry = cls._registry.get(name)
        return entry['func'] if entry else None

    @classmethod
    def get_job_defaults(cls, name):
        """Return the default schedule config for a registered job."""
        entry = cls._registry.get(name)
        return entry.get('defaults', {}) if entry else {}

    @classmethod
    def get_all_jobs(cls):
        """Return {name: func} dict for backward compatibility."""
        return {name: entry['func'] for name, entry in cls._registry.items()}

    @classmethod
    def get_all_entries(cls):
        """Return the full registry with defaults."""
        return cls._registry

# Initializer for the decorator
def zenpulse_job(name, **defaults):
    """
    Decorator to register a function as a ZenPulse job.
    The job will be auto-created in the database ScheduleConfig
    on the next scheduler sync if it doesn't already exist.

    Usage (basic - defaults to interval 5 minutes):
        @zenpulse_job('my_unique_job_key')
        def my_job_function():
            pass

    Usage (custom interval):
        @zenpulse_job('my_job', trigger='interval', interval_value=30, interval_unit='seconds')
        def my_job_function():
            pass

    Usage (cron schedule):
        @zenpulse_job('my_job', trigger='cron', cron_hour='8', cron_minute='30')
        def my_job_function():
            pass

    Supported default kwargs:
        trigger: 'interval' or 'cron' (default: 'interval')
        enabled: True/False (default: True)
        interval_value: int (default: 5)
        interval_unit: 'seconds'|'minutes'|'hours'|'days'|'weeks' (default: 'minutes')
        cron_minute, cron_hour, cron_day, cron_month, cron_day_of_week: str (default: '*')
        max_instances: int (default: 1)
        coalesce: bool (default: True)
        misfire_grace_time: int in seconds (default: 60)
        log_policy: 'none'|'failures'|'all' (default: 'failures')
    """
    return JobRegistry.register(name, **defaults)
