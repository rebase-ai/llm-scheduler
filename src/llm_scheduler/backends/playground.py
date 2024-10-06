import asyncio
from typing import List, Optional, Dict
from datetime import datetime, timezone
from croniter import croniter

from llm_scheduler.domain.task import Task, ScheduleType
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.storages.protocol import Storage
from llm_scheduler.storages.sqlalchemy import InMemoryStorage
from .base import BaseBackend

class PlaygroundBackend(BaseBackend):
    """
    In-memory backend implementation with real-time scheduling capabilities using asyncio.
    WARNING: This backend is for demonstration and playground purposes only.
    It is not suitable for production use as it stores all data in memory and does not persist across restarts.
    """

    def __init__(self, executor_factory: JobExecutorFactory, storage: Storage = InMemoryStorage()):
        super().__init__(executor_factory, storage)
        self.scheduler_task: Optional[asyncio.Task] = None
        self.is_running: bool = False
        self.job_futures: Dict[str, asyncio.Task] = {}
        self.next_execution_times: Dict[str, datetime] = {}

    def __del__(self):
        """
        Ensure all tasks and jobs are removed when the backend is deleted.
        This helps prevent memory leaks and ensures proper cleanup of the event loop.
        """
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()

    async def start(self):
        """
        Start the backend scheduler.
        """
        if isinstance(self.storage, InMemoryStorage):
            await self.storage.create_tables()
        if not self.is_running:
            self.is_running = True
            self.scheduler_task = asyncio.create_task(self._scheduler_loop())
            print("PlaygroundBackend started.")

    async def stop(self):
        """
        Stop the backend scheduler.
        """
        if self.is_running:
            self.is_running = False
            if self.scheduler_task:
                self.scheduler_task.cancel()
                try:
                    await self.scheduler_task
                except asyncio.CancelledError:
                    pass
            for future in self.job_futures.values():
                if not future.done():
                    future.cancel()
            await asyncio.gather(*self.job_futures.values(), return_exceptions=True)
            self.job_futures.clear()
            print("PlaygroundBackend stopped.")

    def _update_next_execution_time(self, task: Task):
        if task.schedule.type == ScheduleType.RECURRING:
            now = datetime.now(timezone.utc)
            cron = croniter(task.schedule.cron_expression, now, second_at_beginning=True)
            self.next_execution_times[task.id] = cron.get_next(datetime)

    def create_task(self, task: Task) -> str:
        self._update_next_execution_time(task)
        return super().create_task(task)

    async def _scheduler_loop(self):
        """
        Main scheduler loop that checks for tasks to execute using asyncio.
        """
        try:
            while self.is_running:
                now = datetime.now(timezone.utc)
                tasks = await self.list_tasks()
                for task in tasks:
                    if task.is_active:
                        if task.schedule.type == ScheduleType.ONE_TIME:
                            if task.schedule.execution_time <= now:
                                jobs = await self.list_jobs(task.id)
                                if not jobs:
                                    self._schedule_task_execution(task)
                                    await self.update_task(task)
                        elif task.schedule.type == ScheduleType.RECURRING:
                            next_execution = self.next_execution_times.get(task.id)
                            if next_execution and next_execution <= now:
                                self._schedule_task_execution(task)
                                self._update_next_execution_time(task)
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error in scheduler loop: {e}")

    def _schedule_task_execution(self, task: Task):
        """
        Schedule a task for execution by creating a new job and running it through the executor.
        """
        if task.id in self.job_futures and not self.job_futures[task.id].done():
            print(f"Warning: Cancelling unfinished job for task {task.id}")
            self.job_futures[task.id].cancel()

        future = asyncio.create_task(self._execute_task(task))
        self.job_futures[task.id] = future
        future.add_done_callback(lambda f: self._handle_job_completion(task.id, f))

    def _handle_job_completion(self, task_id: str, future: asyncio.Future):
        """
        Handle job completion, including cancellation.
        """
        if future.cancelled():
            asyncio.create_task(self._cancel_running_jobs(task_id))

    async def _cancel_running_jobs(self, task_id: str):
        jobs = await self.list_jobs(task_id)
        for job in jobs:
            if job.status == JobStatus.RUNNING:
                job.set_status(JobStatus.CANCELLED)
                await self._update_job(job)

print("WARNING: PlaygroundBackend is for demonstration purposes only. Do not use in production.")
