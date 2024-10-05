import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator


class ScheduleType(str, Enum):
    ONE_TIME = "one_time"
    RECURRING = "recurring"

class BaseSchedule(BaseModel):
    """
    Base class for all schedule types.
    """
    type: ScheduleType
    description: Optional[str] = Field(None, description="Original schedule description, can store raw content extracted by LLM, timezone info, and other descriptive information")

class OneTimeSchedule(BaseSchedule):
    """
    Defines a one-time schedule for task execution.
    """
    type: ScheduleType = ScheduleType.ONE_TIME
    execution_time: datetime = Field(..., description="Precise datetime for task execution")

class RecurringSchedule(BaseSchedule):
    """
    Specifies a recurring schedule for task execution.
    """
    type: ScheduleType = ScheduleType.RECURRING
    cron_expression: str = Field(..., description="Cron expression defining the recurring execution pattern")
    start_time: Optional[datetime] = Field(None, description="Start time for the recurring schedule")
    end_time: Optional[datetime] = Field(None, description="End time for the recurring schedule")

class Task(BaseModel):
    """
    Encapsulates a task that can be scheduled for execution.
    """
    id: str = Field(default_factory=lambda: f"tsk_{uuid.uuid4().hex[:8]}", description="Unique task identifier")
    name: str = Field(..., description="Task name")
    description: Optional[str] = Field(None, description="Original task description, can store raw content extracted by LLM and other descriptive information")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        description="Task creation timestamp with UTC timezone"
    )
    schedule: Union[OneTimeSchedule, RecurringSchedule] = Field(..., description="Task schedule configuration")
    payload: Dict[str, Any] = Field(..., description="Data payload for task execution")
    payload_schema_name: str = Field(..., description="Name of the schema for the payload")
    meta: Optional[Dict[str, Any]] = Field(default=None, description="Custom metadata for user-defined extensions")
    is_active: bool = Field(default=True, description="Indicates whether the task is active")

    @field_validator('created_at')
    def check_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("Datetime must include a timezone to ensure accurate representation of the user's local time. For applications using a single timezone, consider explicitly specifying UTC+0.")
        return v

    @property
    def is_recurring(self) -> bool:
        return self.schedule.type == ScheduleType.RECURRING

    @property
    def is_one_time(self) -> bool:
        return self.schedule.type == ScheduleType.ONE_TIME
    
    @property
    def schedule_type(self) -> ScheduleType:
        return self.schedule.type

    def deactivate(self) -> None:
        self.is_active = False

    def activate(self) -> None:
        self.is_active = True