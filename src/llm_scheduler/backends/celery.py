from celery import Celery
from numpy import take
from llm_scheduler.domain.task import Task, ScheduleType
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.backends.base import BaseBackend
from redbeat import RedBeatSchedulerEntry
from celery.schedules import crontab

from llm_scheduler.storages.protocol import Storage


class CeleryBackend(BaseBackend):
    app: Celery

    def __init__(self, executor_factory: JobExecutorFactory, storage: Storage, celery_app: Celery):
        super().__init__(executor_factory, storage)
        self.app = celery_app

        @self.app.task
        def execute_task(task_id: str):
            import asyncio

            async def _execute():
                task = await self.get_task(task_id)
                print(f"Executing task {task.id}")
                await self._execute_task(task)
            asyncio.run(_execute())

        self._celery_task = execute_task

        self._tasks = {}
        self._jobs = {}

    async def _create_immediate_task(self, task: Task) -> None:
        self._celery_task.apply_async(args=[task.id])

    async def _create_one_time_task(self, task: Task) -> None:
        eta = task.schedule.execution_time
        self._celery_task.apply_async(args=[task.id], eta=eta)

    async def _create_recurring_task(self, task: Task) -> None:
        cron_items = task.schedule.cron_expression.split()
        if len(cron_items) == 6:
            raise ValueError("Unsupported cron expression with seconds")
        elif len(cron_items) != 5:
            raise ValueError("Invalid cron expression")

        minute, hour, day, month, weekday = cron_items
        entry = RedBeatSchedulerEntry(
            f"crontab_task:{task.id}",
            self._celery_task.name,
            crontab(minute=minute, hour=hour, day_of_month=day,
                    month_of_year=month, day_of_week=weekday),
            args=[task.id],
            app=self.app,
        )
        entry.save()

    async def create_task(self, task: Task) -> str:
        if not self.validate_payload(task):
            raise ValueError("Invalid task payload")

        task_id = await self.storage.create_task(task)

        if task.schedule_type == ScheduleType.IMMEDIATE:
            await self._create_immediate_task(task)
        elif task.schedule_type == ScheduleType.ONE_TIME:
            await self._create_one_time_task(task)
        elif task.schedule_type == ScheduleType.RECURRING:
            await self._create_recurring_task(task)
        else:
            raise ValueError(f"Unsupported schedule type: {task.schedule_type}")

        return task_id

    async def start(self):
        # Implementation for starting the Celery worker would go here
        pass

    async def stop(self):
        # Implementation for stopping the Celery worker would go here
        pass
