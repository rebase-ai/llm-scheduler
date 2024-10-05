import asyncio
from typing import List, Optional, Dict
import uuid
from ..domain.task import Task, ScheduleType
from ..domain.job import Job, JobStatus
from .base import BaseBackend
from datetime import datetime, timezone
from croniter import croniter
from llm_scheduler.executor_factory import JobExecutorFactory

class InMemoryBackend(BaseBackend):
    """
    In-memory backend implementation with real-time scheduling capabilities using asyncio.
    WARNING: This backend is not suitable for production use.
    It stores all data in memory and does not persist across restarts.
    """

    def __init__(self, executor_factory: JobExecutorFactory):
        super().__init__(executor_factory)
        self.tasks: Dict[str, Task] = {}
        self.jobs: Dict[str, List[Job]] = {}
        self.scheduler_task: Optional[asyncio.Task] = None
        self.is_running: bool = False
        self.job_futures: Dict[str, asyncio.Task] = {}
        self.next_execution_times: Dict[str, datetime] = {}

    def __del__(self):
        """
        Ensure all tasks and jobs are removed when the backend is deleted.
        This helps prevent memory leaks and ensures proper cleanup of the event loop.
        """
        self.tasks.clear()
        self.jobs.clear()
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()

    async def start(self):
        """
        Start the backend scheduler.
        """
        if not self.is_running:
            self.is_running = True
            self.scheduler_task = asyncio.create_task(self._scheduler_loop())
            print("InMemoryBackend started.")

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
            # Cancel all running job futures
            for future in self.job_futures.values():
                if not future.done():
                    future.cancel()
            await asyncio.gather(*self.job_futures.values(), return_exceptions=True)
            self.job_futures.clear()
            print("InMemoryBackend stopped.")

    async def _create_task(self, task: Task) -> str:
        self.tasks[task.id] = task
        self.jobs[task.id] = []
        if task.schedule.type == ScheduleType.RECURRING:
            self._update_next_execution_time(task)
        return task.id

    async def get_task(self, task_id: str) -> Optional[Task]:
        return self.tasks.get(task_id)

    async def list_tasks(self, limit: int = 100, offset: int = 0) -> List[Task]:
        return list(self.tasks.values())[offset:offset+limit]

    async def get_recent_job(self, task_id: str) -> Optional[Job]:
        task_jobs = self.jobs.get(task_id, [])
        return task_jobs[-1] if task_jobs else None

    async def _update_task(self, task: Task) -> bool:
        if task.id in self.tasks:
            self.tasks[task.id] = task
            if task.schedule.type == ScheduleType.RECURRING:
                self._update_next_execution_time(task)
            return True
        return False

    async def delete_task(self, task_id: str) -> bool:
        if task_id in self.tasks:
            del self.tasks[task_id]
            del self.jobs[task_id]
            if task_id in self.next_execution_times:
                del self.next_execution_times[task_id]
            return True
        return False

    async def list_jobs(self, task_id: str, limit: int = 10) -> List[Job]:
        return self.jobs.get(task_id, [])[-limit:]

    def _update_next_execution_time(self, task: Task):
        if task.schedule.type == ScheduleType.RECURRING:
            now = datetime.now(timezone.utc)
            cron = croniter(task.schedule.cron_expression, now, second_at_beginning=True)
            self.next_execution_times[task.id] = cron.get_next(datetime)

    async def _scheduler_loop(self):
        """
        Main scheduler loop that checks for tasks to execute using asyncio.
        """
        try:
            while self.is_running:
                now = datetime.now(timezone.utc)
                for task_id, task in self.tasks.items():
                    if task.is_active:
                        if task.schedule.type == ScheduleType.ONE_TIME:
                            if task.schedule.execution_time <= now:
                                self._schedule_task_execution(task)
                                task.is_active = False
                        elif task.schedule.type == ScheduleType.RECURRING:
                            next_execution = self.next_execution_times.get(task.id)
                            if next_execution and next_execution <= now:
                                self._schedule_task_execution(task)
                                self._update_next_execution_time(task)
                await asyncio.sleep(1)  # Check every second
        except asyncio.CancelledError:
            # Handle task cancellation gracefully
            pass
        except Exception as e:
            # Log any unexpected exceptions
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
            jobs = self.jobs.get(task_id, [])
            for job in jobs:
                if job.status == JobStatus.RUNNING:
                    job.set_status(JobStatus.CANCELLED)

    async def _execute_task(self, task: Task):
        """
        Execute a task by creating a new job and running it through the executor.
        """
        job = Job(task=task)
        
        if task.id not in self.jobs:
            self.jobs[task.id] = []
        self.jobs[task.id].append(job)
        
        job.set_status(JobStatus.RUNNING)
        
        try:
            executor = self.executor_factory.get_executor(task.payload_schema_name, task.payload)
            await executor.async_execute(job)
            job.set_status(JobStatus.COMPLETED)
        except asyncio.CancelledError:
            job.set_status(JobStatus.CANCELLED)
        except Exception as e:
            job.set_result(str(e), status=JobStatus.FAILED)
            print(f"Error executing task {task.id}: {e}")
        
        self.tasks[task.id] = task

# Warning message
print("WARNING: InMemoryBackend is for demonstration purposes only. Do not use in production.")
