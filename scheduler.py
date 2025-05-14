from datetime import datetime, time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


class ScheduleManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        print("Schedule Manager initialized and started")

    def set_turn_on_job(self, rm_id: str, cron: CronTrigger, job, start_time, end_time):
        print(f"A turn on job is registered with CRON {cron}")
        self.scheduler.add_job(job, trigger=cron, args=[rm_id, start_time, end_time], name="turn_on")

    def set_turn_off_job(self, rm_id: str, cron: CronTrigger, job):
        print(f"A turn off job is registered with CRON {cron}")
        self.scheduler.add_job(job, trigger=cron, args=[rm_id], name="turn_off")

    def set_warning_job(self, job):
        print("A periodic warning job is registered")
        self.scheduler.add_job(job, trigger=IntervalTrigger(seconds=1))

    def purge_all_jobs(self):
        print("Clearing all jobs")
        self.scheduler.remove_all_jobs()

    def register_pub_job_for_settings(self, constant_publish_job):
        self.scheduler.add_job(
            constant_publish_job,
            trigger=IntervalTrigger(seconds=2)
        )


def parse_time(raw_time):
    return datetime.strptime(raw_time, "%I:%M%p").time()


def generate_cron_trig(time_arg: time, full_day_name: str) -> str:
    return f"{time_arg.minute} {time_arg.hour} * * {full_day_name[:3].upper()}"
