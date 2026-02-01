from django.contrib import admin
from .models import ScheduleConfig, JobExecutionLog
from .registry import JobRegistry

@admin.register(ScheduleConfig)
class ScheduleConfigAdmin(admin.ModelAdmin):
    list_display = (
        'job_key', 'enabled', 'trigger_type', 'schedule_display', 
        'log_policy', 'updated_at'
    )
    list_filter = ('enabled', 'trigger_type', 'log_policy')
    search_fields = ('job_key',)
    
    def schedule_display(self, obj):
        if obj.trigger_type == 'interval':
            return f"Every {obj.interval_value} {obj.interval_unit}"
        else:
            return f"{obj.cron_minute} {obj.cron_hour} {obj.cron_day} {obj.cron_month} {obj.cron_day_of_week}"
    
    schedule_display.short_description = "Schedule"

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        if db_field.name == 'job_key':
             # Populate with registered jobs dynamically?
             # Standard CharField with choices is tricky if we want to allow typing new ones (if code updated but registry not loaded in admin process context fully).
             # But if we can inspect registry, we can offer choices.
             # Admin runs in WSGI, Registry jobs might be loaded if apps.ready imports them?
             # Usually jobs are in app/jobs.py. If those aren't imported, Registry is empty.
             # Let's simple Text Input for now with help_text.
             pass
        return super().formfield_for_choice_field(db_field, request, **kwargs)

@admin.register(JobExecutionLog)
class JobExecutionLogAdmin(admin.ModelAdmin):
    list_display = ('job_key', 'status', 'run_time_display', 'duration_ms')
    list_filter = ('job_key', 'status', 'run_time')
    readonly_fields = ('run_time', 'traceback', 'exception_message', 'hostname', 'pid')

    def run_time_display(self, obj):
        return obj.run_time.strftime("%Y-%m-%d %H:%M:%S")
    
    run_time_display.admin_order_field = 'run_time'
    run_time_display.short_description = 'Run Time'
    
    def has_add_permission(self, request):
        return False
    
    def has_change_permission(self, request, obj=None):
        return False
