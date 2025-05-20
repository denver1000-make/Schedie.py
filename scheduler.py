from datetime import datetime, time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from mqtt_topics import TURN_ON_BASE_TOPIC

TURN_ON_JOB = "turn_on"
TURN_OFF_JOB = "turn_off"


class ScheduleManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        print("Schedule Manager initialized and started")

    def set_turn_on_job(self, rm_id: str, cron: CronTrigger, job, start_time, end_time, day_name: str):
        print(f"A turn on job is registered with CRON {cron}")
        job_name = gen_job_name(job_type=TURN_ON_JOB, start_time=start_time, end_time=end_time, room_id=rm_id, day_name=day_name)
        self.scheduler.add_job(job, trigger=cron, args=[rm_id, start_time, end_time, day_name], id=job_name)

    def set_turn_off_job(self, rm_id: str, cron: CronTrigger, job, start_time, end_time, day_name: str):
        print(f"A turn off job is registered with CRON {cron}")
        job_name = gen_job_name(job_type=TURN_OFF_JOB, start_time=start_time, end_time=end_time, room_id=rm_id, day_name=day_name)
        self.scheduler.add_job(job, trigger=cron, args=[rm_id, start_time, end_time, day_name], id=job_name)

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

    def look_for_job_within_gap_and_job_type(
            self,
            job_type: str,
            room_id: str,
            start_time,
            end_time,
            minute_to_ignore_next_start_job,
            now,
            day_name: str
    ) -> bool:
        job_name = gen_job_name(job_type=job_type,
                                room_id=room_id,
                                start_time=start_time,
                                end_time=end_time,
                                day_name=day_name)
        job = self.scheduler.get_job(job_name)

        if job and job.next_run_time:
            gap = (job.next_run_time - now).total_seconds() / 60.0
            return minute_to_ignore_next_start_job <= gap

        return False


def gen_job_name(job_type: str, room_id: str, start_time, end_time, day_name: str) -> str:
    return f"{job_type}_{room_id}_{start_time}_{end_time}_{day_name}"


def parse_time(raw_time):
    return datetime.strptime(raw_time, "%I:%M%p").time()


def generate_cron_trig(time_arg: time, full_day_name: str) -> str:
    return f"{time_arg.minute} {time_arg.hour} * * {full_day_name[:3].upper()}"
