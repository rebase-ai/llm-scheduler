import asyncio
from typing import Dict, List, Type
from pydantic import BaseModel
import pytest
from llm_scheduler.backends.in_memory import InMemoryBackend
from llm_scheduler.domain.job import JobStatus, Job
from llm_scheduler.domain.task import Task, OneTimeSchedule, RecurringSchedule, ScheduleType
from datetime import datetime, timedelta, timezone
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.executors.protocol import JobExecutor


class DummyPayload(BaseModel):
    message: str

class DummyExecutor(JobExecutor):
    @staticmethod
    def supported_schema() -> str:
        return "DummyPayload"

    def execute(self, job: Job) -> None:
        print(f"Executing job {job.id} with payload {job.payload}")

    async def async_execute(self, job: Job) -> None:
        print(f"Executing job {job.id} with payload {job.payload}")

class SlowExecutor(JobExecutor):
    @staticmethod
    def supported_schema() -> str:
        return "DummyPayload"

    async def async_execute(self, job: Job) -> None:
        await asyncio.sleep(3)
        job.set_status(JobStatus.COMPLETED)

@pytest.fixture(scope="function")
def schemas() -> Dict[str, Type[BaseModel]]:
    return {"DummyPayload": DummyPayload}

@pytest.fixture(scope="function")
def executor_factory(schemas: Dict[str, Type[BaseModel]]) -> JobExecutorFactory:
    factory = JobExecutorFactory(schemas)
    factory.register(DummyExecutor)
    return factory

@pytest.fixture(scope="function")
def slow_executor_factory(schemas: Dict[str, Type[BaseModel]]) -> JobExecutorFactory:
    factory = JobExecutorFactory(schemas)
    factory.register(SlowExecutor)
    return factory

@pytest.fixture(scope="function")
def backend(executor_factory):
    return InMemoryBackend(executor_factory)

@pytest.fixture(scope="function")
def slow_backend(slow_executor_factory):
    return InMemoryBackend(slow_executor_factory)

@pytest.fixture(scope="function")
def one_time_task() -> Task:
    return Task(
        id="test_one_time",
        name="One Time Task",
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc) + timedelta(minutes=5)),
        payload_schema_name="DummyPayload",
        payload=DummyPayload(message="One time task data").model_dump(),
        is_active=True
    )

@pytest.fixture(scope="function")
def recurring_task() -> Task:
    return Task(
        id="test_recurring",
        name="Recurring Task",
        schedule=RecurringSchedule(cron_expression="* * * * *"),
        payload_schema_name="DummyPayload",
        payload=DummyPayload(message="Recurring task data").model_dump(),
        is_active=True
    )

@pytest.mark.asyncio
async def test_create_task(backend: InMemoryBackend, one_time_task: Task) -> None:
    task_id: str = await backend.create_task(one_time_task)
    assert task_id == one_time_task.id

    retrieved_task: Task | None = await backend.get_task(task_id)
    assert retrieved_task == one_time_task

@pytest.mark.asyncio
async def test_delay_task(backend: InMemoryBackend, one_time_task: Task) -> None:
    await backend.start()
    # Set the execution time to 1 second from now
    one_time_task.schedule.execution_time = datetime.now(timezone.utc) + timedelta(seconds=1)
    
    task_id: str = await backend.create_task(one_time_task)
    
    # Check that the task is not executed immediately
    initial_job: Job | None = await backend.get_recent_job(task_id)
    assert initial_job is None
    
    # Wait for 4 seconds to ensure the task has been executed
    await asyncio.sleep(4)
    
    # Check that the task has been executed
    executed_job: Job | None = await backend.get_recent_job(task_id)
    assert executed_job is not None
    assert executed_job.status == JobStatus.COMPLETED

    await backend.stop()

@pytest.mark.asyncio
async def test_recurring_task(backend: InMemoryBackend, recurring_task: Task) -> None:
    await backend.start()
    
    # Set the cron expression to run every second
    recurring_task.schedule.cron_expression = "* * * * * *"
    
    task_id: str = await backend.create_task(recurring_task)
    
    # Wait for 2.5 seconds to allow the task to run 3 times
    await asyncio.sleep(2.5)
    
    # Stop the backend
    await backend.stop()
    
    # Check that the task has been executed 3 times
    jobs: List[Job] = await backend.list_jobs(task_id)
    assert len(jobs) == 3
    
    # Check that all jobs are completed
    for job in jobs:
        assert job.status == JobStatus.COMPLETED

@pytest.mark.asyncio
async def test_auto_cancel_previous_job(slow_backend: InMemoryBackend, recurring_task: Task) -> None:
    await slow_backend.start()
    
    recurring_task.schedule.cron_expression = "*/1 * * * * *"

    task_id: str = await slow_backend.create_task(recurring_task)
    
    # Wait for 3.5 seconds to allow two job executions to start
    await asyncio.sleep(3.5)
    
    # Stop the backend
    await slow_backend.stop()
    
    # Check the jobs
    jobs: List[Job] = await slow_backend.list_jobs(task_id)
    assert len(jobs) == 4, "Expected 4 jobs to be created"
    
    assert jobs[0].status == JobStatus.CANCELLED, "Expected first job to be cancelled"
    assert jobs[1].status == JobStatus.CANCELLED, "Expected second job to be cancelled"
    assert jobs[2].status == JobStatus.CANCELLED, "Expected third job to be cancelled"
    assert jobs[3].status == JobStatus.CANCELLED, "Expected fourth job to be cancelled by the backend"

