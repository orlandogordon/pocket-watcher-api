"""Scheduled / operational jobs runnable as modules (e.g. `python -m src.jobs.eod_snapshot`).

These live in tracked source (not scripts/, which is gitignored local tooling)
because the deployment schedules them via cron/systemd (see C5).
"""
