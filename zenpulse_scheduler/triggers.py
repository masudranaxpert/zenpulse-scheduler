from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

def build_trigger(config):
    """
    Builds an APScheduler trigger (IntervalTrigger or CronTrigger) 
    based on the ScheduleConfig model instance.
    """
    if config.trigger_type == 'interval':
        # Default to minutes if not specified or invalid
        unit = config.interval_unit or 'minutes'
        value = config.interval_value or 1
        
        # Mapping unit to arguments for IntervalTrigger
        kwargs = {}
        if unit == 'seconds':
            kwargs['seconds'] = value
        elif unit == 'minutes':
            kwargs['minutes'] = value
        elif unit == 'hours':
            kwargs['hours'] = value
        elif unit == 'days':
            kwargs['days'] = value
        elif unit == 'weeks':
            kwargs['weeks'] = value
        else:
            kwargs['minutes'] = value # Fallback
            
        return IntervalTrigger(**kwargs)

    elif config.trigger_type == 'cron':
        # Use config fields for CronTrigger
        return CronTrigger(
            minute=config.cron_minute,
            hour=config.cron_hour,
            day=config.cron_day,
            month=config.cron_month,
            day_of_week=config.cron_day_of_week,
            timezone=config.timezone if hasattr(config, 'timezone') and config.timezone else None
        )
    
    return None
