from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Type
from pydantic import BaseModel
from llm_scheduler.domain.task import Task
from llm_scheduler.domain.job import Job
from llm_scheduler.executor_factory import JobExecutorFactory

class BaseBackend(ABC):
    def __init__(self, executor_factory: JobExecutorFactory):
        self.executor_factory: JobExecutorFactory = executor_factory
        self.payload_schemas: Dict[str, Type[BaseModel]] = executor_factory.supported_schemas

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
        
    @abstractmethod
    async def _create_task(self, task: Task) -> str:
        pass

    async def create_task(self, task: Task) -> str:
        if not self.validate_payload(task):
            raise ValueError("Invalid task payload")
        return await self._create_task(task)
    
    @abstractmethod
    async def get_task(self, task_id: str) -> Optional[Task]:
        pass

    @abstractmethod
    async def list_tasks(self, limit: int = 100, offset: int = 0) -> List[Task]:
        pass

    @abstractmethod
    async def _update_task(self, task: Task) -> bool:
        pass

    async def update_task(self, task: Task) -> bool:
        if not self.validate_payload(task):
            raise ValueError("Invalid task payload")
        return await self._update_task(task)

    @abstractmethod
    async def delete_task(self, task_id: str) -> bool:
        pass

    @abstractmethod
    async def get_recent_job(self, task_id: str) -> Optional[Job]:
        pass

    @abstractmethod
    async def list_jobs(self, task_id: str, limit: int = 10) -> List[Job]:
        pass

