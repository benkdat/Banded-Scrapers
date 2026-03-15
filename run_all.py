"""
run_all.py — runs all scrapers in sequence and writes a combined log.
Usage: python run_all.py
Set SUPABASE_KEY env var before running.
"""
import subprocess, sys, os, time
from datetime import datetime

# Force UTF-8 output on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

LOG = os.path.join(os.path.dirname(__file__), 'scraper_run.log')

def run(script, label):
    print(f"\n{'='*60}")
    print(f"[{label}] Starting {script} at {datetime.now().strftime('%H:%M:%S')}")
    print('='*60, flush=True)
    start = time.time()
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    proc = subprocess.run(
        [sys.executable, '-u', script],
        env=env,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    elapsed = round(time.time() - start, 1)
    status = 'OK' if proc.returncode == 0 else f'FAILED (exit {proc.returncode})'
    print(f"[{label}] {status} — {elapsed}s", flush=True)
    return proc.returncode == 0

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f"Run started: {datetime.now().isoformat()}")
    print(f"Log: {LOG}", flush=True)

    results = {}
    for script, label in [
        ('scraper.py',              'Job Boards'),
        ('bls_scraper.py',          'BLS OEWS'),
        ('sec_scraper.py',          'SEC Proxy'),
        ('career_page_scraper.py',  'Career Pages T2'),
        ('h1b_scraper.py',          'H-1B'),
        ('perm_scraper.py',         'PERM'),
    ]:
        results[label] = run(script, label)

    print(f"\n{'='*60}")
    print(f"ALL DONE — {datetime.now().strftime('%H:%M:%S')}")
    for label, ok in results.items():
        print(f"  {'✓' if ok else '✗'} {label}")
