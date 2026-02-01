from django.db import models
from django.utils.translation import gettext_lazy as _

class ScheduleConfig(models.Model):
    TRIGGER_CHOICES = (
        ('interval', 'Interval'),
        ('cron', 'Cron'),
    )

    LOG_POLICY_CHOICES = (
        ('none', 'None'),
        ('failures', 'Failures Only'),
        ('all', 'All Executions'),
    )

    INTERVAL_UNIT_CHOICES = (
        ('seconds', 'Seconds'),
        ('minutes', 'Minutes'),
        ('hours', 'Hours'),
        ('days', 'Days'),
        ('weeks', 'Weeks'),
    )

    job_key = models.CharField(
        max_length=255, 
        unique=True, 
        help_text="Unique key identifier for the job (must match the registered job name)."
    )
    enabled = models.BooleanField(default=True)
    trigger_type = models.CharField(max_length=20, choices=TRIGGER_CHOICES, default='interval')
    
    # Interval Fields
    interval_value = models.IntegerField(null=True, blank=True, help_text="Value for interval trigger.")
    interval_unit = models.CharField(
        max_length=20, 
        choices=INTERVAL_UNIT_CHOICES, 
        default='minutes',
        null=True, blank=True
    )

    # Cron Fields
    cron_minute = models.CharField(max_length=100, default='*', help_text="Cron minute (0-59 or *)")
    cron_hour = models.CharField(max_length=100, default='*', help_text="Cron hour (0-23 or *)")
    cron_day = models.CharField(max_length=100, default='*', help_text="Cron day of month (1-31 or *)")
    cron_month = models.CharField(max_length=100, default='*', help_text="Cron month (1-12 or *)")
    cron_day_of_week = models.CharField(max_length=100, default='*', help_text="Cron day of week (0-6 or mon,tue... or *)")

    # Options
    max_instances = models.IntegerField(default=1, help_text="Maximum number of concurrently running instances allowed.")
    coalesce = models.BooleanField(default=True, help_text="Combine missed runs into one.")
    misfire_grace_time = models.IntegerField(default=60, help_text="Seconds after the designated run time that the job is still allowed to run.")
    
    log_policy = models.CharField(max_length=20, choices=LOG_POLICY_CHOICES, default='failures')
    
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.job_key} ({self.trigger_type})"

class JobExecutionLog(models.Model):
    STATUS_CHOICES = (
        ('success', 'Success'),
        ('fail', 'Fail'),
    )

    job_key = models.CharField(max_length=255, db_index=True)
    run_time = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES)
    duration_ms = models.FloatField(help_text="Execution duration in milliseconds")
    
    exception_type = models.CharField(max_length=255, null=True, blank=True)
    exception_message = models.TextField(null=True, blank=True)
    traceback = models.TextField(null=True, blank=True)
    
    hostname = models.CharField(max_length=255, null=True, blank=True)
    pid = models.IntegerField(null=True, blank=True)

    class Meta:
        ordering = ['-run_time']
        indexes = [
            models.Index(fields=['job_key', 'status']),
            models.Index(fields=['run_time']),
        ]

    def __str__(self):
        return f"{self.job_key} - {self.status} at {self.run_time}"
