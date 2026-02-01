import logging

logger = logging.getLogger(__name__)

class JobRegistry:
    _registry = {}

    @classmethod
    def register(cls, name):
        def decorator(func):
            if name in cls._registry:
                logger.warning(f"Job with key '{name}' already registered. Overwriting.")
            cls._registry[name] = func
            return func
        return decorator

    @classmethod
    def get_job(cls, name):
        return cls._registry.get(name)

    @classmethod
    def get_all_jobs(cls):
        return cls._registry

# Initializer for the decorator
def zenpulse_job(name):
    """
    Decorator to register a function as a ZenPulse job.
    Usage:
    @zenpulse_job('my_unique_job_key')
    def my_job_function():
        pass
    """
    return JobRegistry.register(name)
