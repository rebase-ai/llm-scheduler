import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union
import logging
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator


class ScheduleType(str, Enum):
    IMMEDIATE = "immediate"
    ONE_TIME = "one_time"
    RECURRING = "recurring"

class BaseSchedule(BaseModel):
    """
    Base class for all schedule types.
    """
    type: ScheduleType
    description: Optional[str] = Field(None, description="Original schedule description, can store raw content extracted by LLM, timezone info, and other descriptive information")

class ImmediateSchedule(BaseSchedule):
    """
    Defines an immediate schedule for task execution.
    """
    type: ScheduleType = ScheduleType.IMMEDIATE

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
    schedule: Union[ImmediateSchedule, OneTimeSchedule, RecurringSchedule] = Field(..., description="Task schedule configuration")
    payload: Dict[str, Any] = Field(..., description="Data payload for task execution")
    payload_schema_name: str = Field(..., description="Name of the schema for the payload")
    meta: Optional[Dict[str, Any]] = Field(default=None, description="Custom metadata for user-defined extensions")
    is_active: bool = Field(default=True, description="Indicates whether the task is active")

    @field_validator('created_at')
    def check_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            logging.warning("Datetime does not include a timezone. Defaulting to UTC+0 for consistent representation. "
                          "Note: When using SQLite for storage, timezone information may be automatically discarded. "
                          "If preserving original timezone information is crucial for your business logic, "
                          "consider using a database that supports timezone storage, such as PostgreSQL.")
            return v.replace(tzinfo=ZoneInfo("UTC"))
        return v

    @property
    def is_recurring(self) -> bool:
        return self.schedule.type == ScheduleType.RECURRING

    @property
    def is_one_time(self) -> bool:
        return self.schedule.type == ScheduleType.ONE_TIME

    @property
    def is_immediate(self) -> bool:
        return self.schedule.type == ScheduleType.IMMEDIATE
    
    @property
    def schedule_type(self) -> ScheduleType:
        return self.schedule.type

    def deactivate(self) -> None:
        self.is_active = False

    def activate(self) -> None:
        self.is_active = True