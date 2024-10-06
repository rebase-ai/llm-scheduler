import uuid
from enum import Enum
from typing import Any, Dict, Optional
from datetime import datetime

from pydantic import BaseModel, Field

from .task import Task


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class Job(BaseModel):
    """
    Represents a scheduled task execution.
    """
    id: str = Field(default_factory=lambda: f"job_{uuid.uuid4().hex[:8]}", description="Unique job identifier")
    task: Task = Field(..., description="The task associated with this job")
    status: JobStatus = JobStatus.PENDING
    result: Optional[Any] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def payload(self) -> Optional[Dict[str, Any]]:
        return self.task.payload

    @property
    def meta(self) -> Optional[Dict[str, Any]]:
        return self.task.meta
    
    def set_status(self, status: JobStatus):
        """
        Update the status of the job.
        """
        self.status = status
        if status == JobStatus.RUNNING:
            self.start_time = datetime.now()
        elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            self.end_time = datetime.now()

    def set_result(self, result: Any, status: JobStatus = JobStatus.COMPLETED):
        """
        Set the result of the job execution.
        """
        if status not in [JobStatus.COMPLETED, JobStatus.FAILED]:
            raise ValueError("Status must be either COMPLETED or FAILED")
        self.result = result
        self.set_status(status)
