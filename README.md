# ⚡ ZenPulse Scheduler for Django

**Modern, Hybrid, and Zero-Latency Job Scheduler**

ZenPulse Scheduler is a **Developer-First** background job scheduler for Django. Unlike traditional schedulers that either rely on heavy external infrastructure (Redis/Celery) or spam your database with polling queries, ZenPulse uses a **Smart Hybrid Architecture**.

---

## 🌟 Features

- ✅ **No Redis/Celery Required** - Pure Django solution
- ✅ **Zero DB Impact** - Smart file-based signaling system
- ✅ **Hybrid Execution** - Config in DB, Runtime in RAM
- ✅ **Selective Logging** - Log everything, nothing, or only failures
- ✅ **Live Updates** - Change schedules without restarting
- ✅ **Multi-Process Safe** - Built-in locking mechanisms
- ✅ **Interval & Cron Support** - Flexible scheduling options

---

## 📦 Installation

### Step 1: Install the Package

```bash
pip install zenpulse-scheduler
```

Or if installing from source:

```bash
cd /path/to/package
pip install -e .
```

### Step 2: Add to Django Settings

```python
# settings.py
INSTALLED_APPS = [
    # ... other apps
    'zenpulse_scheduler',
]
```

### Step 3: Run Migrations

```bash
python manage.py migrate
```

---

## 🚀 Quick Start Guide

### 1. Define Your Job

Create a file (e.g., `yourapp/jobs.py`) and register your job:

```python
from zenpulse_scheduler.registry import zenpulse_job

@zenpulse_job("send_daily_report")
def send_daily_report():
    """Send daily sales report via email"""
    print("📧 Sending daily report...")
    # Your business logic here
```

**Important:** Make sure this file is imported when Django starts. You can do this by:

```python
# yourapp/apps.py
from django.apps import AppConfig

class YourAppConfig(AppConfig):
    name = 'yourapp'
    
    def ready(self):
        import yourapp.jobs  # Import to register jobs
```

Or import it in your `urls.py`:

```python
# yourapp/urls.py
import yourapp.jobs  # Ensure jobs are registered

urlpatterns = [
    # your urls
]
```

### 2. Start the Scheduler

Open a **separate terminal** and run:

```bash
python manage.py run_zenpulse_scheduler
```

You should see:

```
Starting ZenPulse Scheduler (Sync: 10s, Lock: False)...
```

### 3. Configure via Admin

1. Go to **Django Admin** → **ZenPulse Scheduler** → **Schedule Configs**
2. Click **Add Schedule Config**
3. Fill in the details (see Configuration Guide below)
4. Save

Your job will start running automatically!

---

## 📚 Complete Configuration Guide

All job scheduling is controlled through the **ScheduleConfig** model in Django Admin.

### Core Fields

#### **Job Key** (Required)
- **Type:** Text (Unique)
- **Description:** Must match the name you used in `@zenpulse_job("name")`
- **Example:** `send_daily_report`

#### **Enabled** (Required)
- **Type:** Checkbox
- **Description:** Controls whether the job runs
- **Default:** ✅ Checked (Enabled)
- **Behavior:**
  - ✅ Checked → Job runs according to schedule
  - ❌ Unchecked → Job stops immediately (within next sync interval)

---

### Trigger Configuration

#### **Trigger Type** (Required)

Choose how your job should be scheduled:

| Choice | When to Use | Example |
|--------|-------------|---------|
| **Interval** | Repeating tasks that run in a loop | "Check for new emails every 5 minutes" |
| **Cron** | Tasks that run at specific calendar times | "Send report every Monday at 9 AM" |

---

### Interval Configuration

Use when **Trigger Type** = `Interval`

#### **Interval Value** (Required for Interval)
- **Type:** Number
- **Description:** How many units to wait between runs
- **Example:** `30` (combined with unit below)

#### **Interval Unit** (Required for Interval)

| Unit | Best For | Example Usage |
|------|----------|---------------|
| **Seconds** | High-frequency monitoring, heartbeats | `30` seconds = runs every 30 seconds |
| **Minutes** | Standard periodic tasks | `15` minutes = runs every 15 minutes |
| **Hours** | Regular updates, data sync | `4` hours = runs every 4 hours |
| **Days** | Daily routines | `1` day = runs once per day |
| **Weeks** | Weekly maintenance | `2` weeks = runs every 2 weeks |

**Example Configurations:**

```
Interval Value: 10, Unit: Minutes → Runs every 10 minutes
Interval Value: 1, Unit: Hours → Runs every hour
Interval Value: 30, Unit: Seconds → Runs every 30 seconds
```

---

### Cron Configuration

Use when **Trigger Type** = `Cron`

All cron fields support:
- Specific values (e.g., `8` for 8 AM)
- Wildcards (`*` means "every")
- Lists (e.g., `mon,wed,fri`)
- Ranges (e.g., `1-5`)

#### **Cron Minute**
- **Valid Values:** `0-59` or `*`
- **Default:** `*` (every minute)
- **Examples:**
  - `0` = At the top of the hour
  - `30` = At 30 minutes past the hour
  - `*/15` = Every 15 minutes

#### **Cron Hour**
- **Valid Values:** `0-23` or `*`
- **Default:** `*` (every hour)
- **Examples:**
  - `9` = 9 AM
  - `14` = 2 PM
  - `22` = 10 PM

#### **Cron Day (of Month)**
- **Valid Values:** `1-31` or `*`
- **Default:** `*` (every day)
- **Examples:**
  - `1` = 1st of the month
  - `15` = 15th of the month

#### **Cron Month**
- **Valid Values:** `1-12` or `*`
- **Default:** `*` (every month)
- **Examples:**
  - `1` = January
  - `12` = December

#### **Cron Day of Week**
- **Valid Values:** `0-6` (0=Sunday) or `mon,tue,wed,thu,fri,sat,sun` or `*`
- **Default:** `*` (every day)
- **Examples:**
  - `mon` = Every Monday
  - `0` = Every Sunday
  - `mon,wed,fri` = Monday, Wednesday, Friday

**Example Cron Configurations:**

```
Daily at 8:30 AM:
  Minute: 30, Hour: 8, Day: *, Month: *, Day of Week: *

Every Monday at 9 AM:
  Minute: 0, Hour: 9, Day: *, Month: *, Day of Week: mon

First day of every month at midnight:
  Minute: 0, Hour: 0, Day: 1, Month: *, Day of Week: *

Every weekday at 6 PM:
  Minute: 0, Hour: 18, Day: *, Month: *, Day of Week: mon,tue,wed,thu,fri
```

---

### Advanced Options

#### **Max Instances**
- **Type:** Number
- **Default:** `1`
- **Description:** Maximum number of this job that can run simultaneously
- **Use Case:** Set to `3` if you want to allow up to 3 parallel executions

#### **Coalesce**
- **Type:** Checkbox
- **Default:** ✅ Checked
- **Description:** If multiple runs were missed (e.g., scheduler was down), combine them into one
- **Behavior:**
  - ✅ Checked → Run once to catch up
  - ❌ Unchecked → Run all missed executions

#### **Misfire Grace Time**
- **Type:** Number (seconds)
- **Default:** `60`
- **Description:** How long after the scheduled time the job can still run
- **Example:** If job was supposed to run at 10:00 but scheduler was busy, it can still run until 10:01 (60 seconds grace)

---

### Logging Configuration

#### **Log Policy**

Control how much execution history is saved to the database:

| Policy | What Gets Logged | Database Impact | Best For |
|--------|------------------|-----------------|----------|
| **None** | Nothing | Zero writes | High-frequency jobs (every few seconds) |
| **Failures Only** | Only when job crashes | Minimal writes | Production critical jobs (recommended) |
| **All Executions** | Every success and failure | High writes | Debugging, auditing |

**Recommendation:** Use `Failures Only` for production. This way:
- ✅ You get full error traces when something breaks
- ✅ Database stays clean
- ✅ You can still monitor job health

---

## 🖥️ Admin Panel Examples

### Example 1: Send Email Every 30 Minutes

```
Job Key: send_email_digest
Enabled: ✅
Trigger Type: Interval
Interval Value: 30
Interval Unit: Minutes
Log Policy: Failures Only
```

### Example 2: Daily Report at 8:30 AM

```
Job Key: generate_daily_report
Enabled: ✅
Trigger Type: Cron
Cron Minute: 30
Cron Hour: 8
Cron Day: *
Cron Month: *
Cron Day of Week: *
Log Policy: All Executions
```

### Example 3: Weekly Cleanup Every Sunday at Midnight

```
Job Key: weekly_cleanup
Enabled: ✅
Trigger Type: Cron
Cron Minute: 0
Cron Hour: 0
Cron Day: *
Cron Month: *
Cron Day of Week: sun
Log Policy: Failures Only
```

---

## ⚙️ Running the Scheduler

### Development

```bash
python manage.py run_zenpulse_scheduler
```

### Production (with Safety Lock)

```bash
python manage.py run_zenpulse_scheduler --lock
```

The `--lock` flag prevents multiple scheduler instances from running simultaneously:
- **PostgreSQL:** Uses advisory locks (recommended)
- **MySQL:** Uses `GET_LOCK`
- **SQLite/Others:** Uses file-based locking

### Custom Sync Interval

By default, the scheduler checks for config changes every 10 seconds. To reduce database queries:

```bash
python manage.py run_zenpulse_scheduler --sync-every 60
```

This checks only once per minute. Trade-off: Changes take up to 60 seconds to apply.

---

## 🏗️ Architecture

```
┌─────────────────┐
│  Django Admin   │  ← You configure jobs here
└────────┬────────┘
         │ Save Config
         ↓
┌─────────────────┐
│    Database     │  ← Stores ScheduleConfig
└────────┬────────┘
         │ Sync (every 10s)
         ↓
┌─────────────────┐
│   Scheduler     │  ← Runs in separate process
│   (Memory)      │
└────────┬────────┘
         │ Execute
         ↓
┌─────────────────┐
│   Your Job      │  ← @zenpulse_job decorated function
└─────────────────┘
```

**Key Points:**
1. Configuration lives in **Database** (persistent)
2. Execution happens in **Memory** (fast)
3. Scheduler syncs config changes automatically
4. No Redis or external dependencies needed

---

## 📊 Monitoring Job Execution

### View Execution Logs

Go to **Django Admin** → **ZenPulse Scheduler** → **Job Execution Logs**

You'll see:
- **Job Key:** Which job ran
- **Status:** Success or Fail
- **Run Time:** When it executed (with seconds precision)
- **Duration:** How long it took (in milliseconds)
- **Exception Details:** Full traceback if it failed

### Understanding Log Entries

```
Job Key: send_email_digest
Status: Success
Run Time: 2026-02-01 14:30:45
Duration: 234.5 ms
```

If a job fails:

```
Job Key: send_email_digest
Status: Fail
Run Time: 2026-02-01 14:30:45
Exception Type: SMTPException
Exception Message: Connection refused
Traceback: [Full Python traceback here]
```

---

## 🔧 Troubleshooting

### Job Not Running?

1. **Check if job is registered:**
   ```python
   # In Django shell
   from zenpulse_scheduler.registry import JobRegistry
   print(JobRegistry.get_all_jobs())
   ```
   Your job should appear in the list.

2. **Check if enabled in Admin:**
   - Go to Schedule Configs
   - Verify `Enabled` is checked

3. **Check scheduler is running:**
   - Look for `Starting ZenPulse Scheduler...` in terminal
   - Check for any error messages

### Duplicate Executions?

- Make sure only **one** `run_zenpulse_scheduler` process is running
- Use `--lock` flag in production
- Check if you accidentally started it in multiple terminals

### Jobs Running at Wrong Time?

- Verify your `TIME_ZONE` setting in Django settings
- Check cron configuration carefully
- Use `Interval` for simple repeating tasks

### Database Performance Issues?

- Set `Log Policy` to `None` or `Failures Only`
- Increase `--sync-every` to reduce config checks
- Consider archiving old logs periodically

---

## 🚀 Production Deployment

### Using Systemd (Linux)

Create `/etc/systemd/system/zenpulse-scheduler.service`:

```ini
[Unit]
Description=ZenPulse Scheduler
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/path/to/your/project
ExecStart=/path/to/venv/bin/python manage.py run_zenpulse_scheduler --lock --sync-every 30
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable zenpulse-scheduler
sudo systemctl start zenpulse-scheduler
sudo systemctl status zenpulse-scheduler
```

### Using Supervisor

```ini
[program:zenpulse-scheduler]
command=/path/to/venv/bin/python manage.py run_zenpulse_scheduler --lock
directory=/path/to/your/project
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/zenpulse-scheduler.log
```

### Using Docker

```dockerfile
# In your Dockerfile
CMD ["python", "manage.py", "run_zenpulse_scheduler", "--lock"]
```

Or in `docker-compose.yml`:

```yaml
services:
  scheduler:
    build: .
    command: python manage.py run_zenpulse_scheduler --lock
    depends_on:
      - db
```

---

## ❓ FAQ

**Q: Can I run this with Gunicorn/Uvicorn?**  
A: Yes, but run the scheduler in a **separate process/container**. Never run it inside web workers.

**Q: What happens if I restart the server?**  
A: Configuration is in the database, so it persists. Just start the scheduler command again.

**Q: Can jobs accept parameters?**  
A: Currently, jobs should be parameter-less functions. For different parameters, create separate job functions.

**Q: How do I delete old logs?**  
A: You can manually delete from Admin or create a cleanup job:
```python
@zenpulse_job("cleanup_old_logs")
def cleanup_old_logs():
    from datetime import timedelta
    from django.utils import timezone
    from zenpulse_scheduler.models import JobExecutionLog
    
    cutoff = timezone.now() - timedelta(days=30)
    JobExecutionLog.objects.filter(run_time__lt=cutoff).delete()
```

**Q: Is this production-ready?**  
A: Yes! It's built on APScheduler (battle-tested library) with Django best practices.

---

## 📝 License

[Your License Here]

## 🤝 Contributing

Contributions welcome! Please open an issue or PR.

---

**Made with ❤️ for Django developers who want simple, powerful scheduling.**
