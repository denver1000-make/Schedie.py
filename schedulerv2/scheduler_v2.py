import dataclasses
import uuid
from datetime import datetime, time

from apscheduler.job import Job
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

TURN_ON_JOB = "turn_on"
TURN_OFF_JOB = "turn_off"


@dataclasses.dataclass
class JobPair:
    start_job: Job
    turn_off_job: Job


def init_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.start()
    print("Schedule Manager initialized and started")
    return scheduler


def set_turn_on_job(scheduler: BackgroundScheduler, rm_id: str, cron: CronTrigger, job: Job, start_time, end_time,
                    day_name: str):
    print(f"A turn on job is registered with CRON {cron}")
    job_name = gen_job_name(job_type=TURN_ON_JOB, start_time=start_time, end_time=end_time, room_id=rm_id,
                            day_name=day_name)
    scheduler.add_job(job, trigger=cron, args=[rm_id, start_time, end_time, day_name], id=job_name)


def set_turn_off_job(scheduler: BackgroundScheduler, rm_id: str, cron: CronTrigger, job: Job, start_time, end_time,
                     day_name: str):
    print(f"A turn off job is registered with CRON {cron}")
    job_name = gen_job_name(job_type=TURN_OFF_JOB, start_time=start_time, end_time=end_time, room_id=rm_id,
                            day_name=day_name)
    scheduler.add_job(job, trigger=cron, args=[rm_id, start_time, end_time, day_name], id=job_name)


def set_warning_job(scheduler: BackgroundScheduler, job):
    print("A periodic warning job is registered")
    scheduler.add_job(job, trigger=IntervalTrigger(seconds=1))


def purge_all_jobs(scheduler: BackgroundScheduler):
    print("Clearing all jobs")
    scheduler.remove_all_jobs()


def register_pub_job_for_settings(scheduler: BackgroundScheduler, constant_publish_job):
    scheduler.add_job(
        constant_publish_job,
        trigger=IntervalTrigger(seconds=2)
    )


def look_for_job_within_gap_and_job_type(
        scheduler: BackgroundScheduler,
        job_type: str,
        room_id: str,
        start_time: str,
        end_time: str,
        minute_to_ignore_next_start_job: int,
        now: datetime,
        day_name: str
) -> bool:
    all_jobs = scheduler.get_jobs()

    current_job_id = gen_job_name(job_type, room_id, start_time, end_time, day_name)

    for job in all_jobs:
        job_id = job.id
        if not job.next_run_time:
            continue

        if job_id == current_job_id:
            continue

        if not job_id.startswith(job_type):
            continue

        parts = job_id.split('/')
        if len(parts) < 5:
            continue

        _, job_room_id, job_start, job_end, job_day = parts

        if (
                job_room_id == room_id and
                job_day.lower() == day_name.lower()
        ):
            gap_minutes = (job.next_run_time - now).total_seconds() / 60.0
            if 0 <= gap_minutes <= minute_to_ignore_next_start_job:
                return True

    return False


#
# def gen_job_name(job_type: str, room_id: str, start_time, end_time, day_name: str) -> str:
#     return f"{job_type}/{room_id}/{start_time}/{end_time}/{day_name}"

def gen_job_name(
        job_type: str,
        room_id: str,
        start_time: str,
        end_time: str,
        day_name: str,
        unique: bool = True
) -> str:
    """
    Generate a unique job ID for APScheduler.

    Format:
      {job_type}/{room_id}/{start_time}/{end_time}/{day_name}[-<suffix>]

    If unique=True, appends a short UUID suffix to prevent duplication.
    """
    base_id = f"{job_type}/{room_id}/{start_time}/{end_time}/{day_name}"
    if unique:
        suffix = uuid.uuid4().hex[:6]  # 6-char hex for brevity
        return f"{base_id}-{suffix}"
    return base_id


def parse_time(raw_time):
    return datetime.strptime(raw_time, "%I:%M%p").time()


def generate_cron_trig(time_arg: time, full_day_name: str) -> str:
    return f"{time_arg.minute} {time_arg.hour} * * {full_day_name[:3].upper()}"
