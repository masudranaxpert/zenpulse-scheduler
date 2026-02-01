import sys
import atexit
import os
import logging
from django.db import connection

logger = logging.getLogger(__name__)

class BaseLock:
    def acquire(self):
        raise NotImplementedError
    
    def release(self):
        raise NotImplementedError

class PIDFileLock(BaseLock):
    def __init__(self, key="zenpulse_scheduler"):
        self.lockfile = f"/tmp/{key}.lock" if sys.platform != "win32" else f"{os.getenv('TEMP')}/{key}.lock"
        self._f = None

    def acquire(self):
        try:
            if os.path.exists(self.lockfile):
                # Check if pid exists
                with open(self.lockfile, 'r') as f:
                    pid = int(f.read().strip())
                
                # Check if process is running
                try:
                    # Signal 0 checks if process exists (Unix) or OpenProcess (Windows)
                    os.kill(pid, 0)
                    logger.warning(f"Lock file exists and process {pid} is running.")
                    return False
                except OSError:
                    # Process dead, safe to take over
                    pass
            
            self._f = open(self.lockfile, 'w')
            self._f.write(str(os.getpid()))
            self._f.flush()
            atexit.register(self.release)
            return True
        except Exception as e:
            logger.error(f"Failed to acquire PID lock: {e}")
            return False

    def release(self):
        try:
            if self._f:
                self._f.close()
                os.remove(self.lockfile)
                self._f = None
        except Exception:
            pass

class DatabaseAdvisoryLock(BaseLock):
    """
    Attempts to use DB-specific advisory locks.
    Supports PostgreSQL and MySQL.
    """
    LOCK_ID = 808080 # arbitrary integer for advisory lock

    def __init__(self):
        self.acquired = False

    def acquire(self):
        if connection.vendor == 'postgresql':
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s)", [self.LOCK_ID])
                row = cursor.fetchone()
                if row and row[0]:
                    self.acquired = True
                    return True
                return False
        elif connection.vendor == 'mysql':
            with connection.cursor() as cursor:
                # GET_LOCK returns 1 if success, 0 if timeout, NULL on error
                cursor.execute("SELECT GET_LOCK(%s, 0)", [str(self.LOCK_ID)])
                row = cursor.fetchone()
                if row and row[0] == 1:
                    self.acquired = True
                    return True
                return False
        else:
            logger.warning("DatabaseAdvisoryLock not supported for this vendor. Falling back to PID lock.")
            return PIDFileLock().acquire() # Fallback

    def release(self):
        if not self.acquired:
            return

        if connection.vendor == 'postgresql':
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [self.LOCK_ID])
        elif connection.vendor == 'mysql':
            with connection.cursor() as cursor:
                cursor.execute("SELECT RELEASE_LOCK(%s)", [str(self.LOCK_ID)])
        
        self.acquired = False

def get_best_lock():
    # Prefer DB lock usually, but for simplicity/universality check config or default to PID?
    # User asked for DB lock optional.
    # Let's try DB lock, if not supported (sqlite), fallback to PID.
    if connection.vendor in ('postgresql', 'mysql'):
        return DatabaseAdvisoryLock()
    return PIDFileLock()
