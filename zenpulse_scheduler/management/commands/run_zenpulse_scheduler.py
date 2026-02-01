from django.core.management.base import BaseCommand
from zenpulse_scheduler.engine import ZenPulseEngine

class Command(BaseCommand):
    help = 'Runs the ZenPulse Scheduler'

    def add_arguments(self, parser):
        parser.add_argument(
            '--sync-every',
            type=int,
            default=10,
            help='Seconds between DB config syncs (default: 10)'
        )
        parser.add_argument(
            '--lock',
            action='store_true',
            help='Enable single-instance locking (DB or File)'
        )

    def handle(self, *args, **options):
        sync_interval = options['sync_every']
        use_lock = options['lock']
        
        self.stdout.write(self.style.SUCCESS(f"Starting ZenPulse Scheduler (Sync: {sync_interval}s, Lock: {use_lock})..."))
        
        engine = ZenPulseEngine(sync_interval=sync_interval, use_lock=use_lock)
        engine.start()
