from typing import List, Optional, Protocol
from llm_scheduler.domain.task import Task
from llm_scheduler.domain.job import Job

class Storage(Protocol):
    async def create_task(self, task: Task) -> str:
        """Create a new task and return its ID."""
        ...

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by its ID."""
        ...

    async def update_task(self, task: Task) -> bool:
        """Update an existing task. Return True if successful, False otherwise."""
        ...

    async def delete_task(self, task_id: str) -> bool:
        """Delete a task by its ID. Return True if successful, False otherwise."""
        ...

    async def list_tasks(self, limit: int = 100, offset: int = 0) -> List[Task]:
        """List tasks with pagination."""
        ...

    async def create_job(self, job: Job) -> str:
        """Create a new job and return its ID."""
        ...

    async def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieve a job by its ID."""
        ...

    async def update_job(self, job: Job) -> bool:
        """Update an existing job. Return True if successful, False otherwise."""
        ...

    async def list_recent_jobs(self, task_id: str, limit: int = 10) -> List[Job]:
        """List jobs for a specific task with limit order by start_time descending"""
        ...

    async def get_recent_job(self, task_id: str) -> Optional[Job]:
        """Get the most recent job for a specific task."""
        ...
