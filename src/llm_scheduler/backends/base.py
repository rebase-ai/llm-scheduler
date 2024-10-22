from abc import ABC, abstractmethod
import asyncio
from typing import List, Optional, Dict, Type
from pydantic import BaseModel
from llm_scheduler.domain.task import Task
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.storages.protocol import Storage

class BaseBackend(ABC):
    def __init__(self, executor_factory: JobExecutorFactory, storage: Storage):
        self.executor_factory: JobExecutorFactory = executor_factory
        self.payload_schemas: Dict[str, Type[BaseModel]] = executor_factory.supported_schemas
        self.storage: Storage = storage

    def validate_payload(self, task: Task) -> bool:
        if not task.payload_schema_name or not task.payload:
            return False
        
        schema = self.payload_schemas.get(task.payload_schema_name)
        if not schema:
            return False
        
        try:
            schema(**task.payload)
            return True
        except ValueError:
            return False
        
    async def start(self):
        pass

    async def stop(self):
        pass

    async def create_task(self, task: Task) -> str:
        if not self.validate_payload(task):
            raise ValueError("Invalid task payload")
        return await self.storage.create_task(task)
    
    async def get_task(self, task_id: str) -> Optional[Task]:
        return await self.storage.get_task(task_id)

    async def list_tasks(self, limit: int = 100, offset: int = 0) -> List[Task]:
        return await self.storage.list_tasks(limit, offset)

    async def _update_task(self, task: Task) -> bool:
        return await self.storage.update_task(task)

    async def update_task(self, task: Task) -> bool:
        if not self.validate_payload(task):
            raise ValueError("Invalid task payload")
        return await self._update_task(task)

    async def delete_task(self, task_id: str) -> bool:
        return await self.storage.delete_task(task_id)

    async def activate_task(self, task_id: str) -> bool:
        task = await self.get_task(task_id)
        if task:
            task.activate()
            return await self._update_task(task)
        return False

    async def deactivate_task(self, task_id: str) -> bool:
        task = await self.get_task(task_id)
        if task:
            task.deactivate()
            return await self._update_task(task)
        return False

    async def get_latest_job(self, task_id: str) -> Optional[Job]:
        return await self.storage.get_recent_job(task_id)

    async def list_recent_jobs(self, task_id: str, limit: int = 10) -> List[Job]:
        return await self.storage.list_recent_jobs(task_id, limit)

    async def _create_job(self, task: Task) -> Job:
        job = Job(task=task)
        await self.storage.create_job(job)
        return job

    async def _update_job(self, job: Job) -> bool:
        return await self.storage.update_job(job)

    async def _execute_task(self, task: Task):
        if not task.is_active:
            return

        job = await self._create_job(task)

        job.set_status(JobStatus.RUNNING)
        await self._update_job(job)
        
        try:
            executor = self.executor_factory.get_executor(task.payload_schema_name, task.payload)
            await executor.async_execute(job)
            job.set_status(JobStatus.COMPLETED)
            await self._update_job(job)
        except asyncio.CancelledError:
            job.set_status(JobStatus.CANCELLED)
            await self._update_job(job)
        except Exception as e:
            job.set_result(str(e), status=JobStatus.FAILED)
            print(f"Error executing task {task.id}: {e}")
            await self._update_job(job)