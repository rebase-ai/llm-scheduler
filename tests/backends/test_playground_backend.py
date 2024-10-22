import asyncio
from typing import Dict, List, Type
from pydantic import BaseModel
import pytest
import pytest_asyncio
from llm_scheduler.backends.playground import PlaygroundBackend
from llm_scheduler.domain.job import JobStatus, Job
from llm_scheduler.domain.task import Task, OneTimeSchedule, RecurringSchedule, ImmediateSchedule, ScheduleType
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


@pytest_asyncio.fixture(scope="function")
async def backend(executor_factory):
    backend = PlaygroundBackend(executor_factory)
    await backend.start()
    yield backend
    await backend.stop()


@pytest_asyncio.fixture(scope="function")
async def slow_backend(slow_executor_factory):
    backend = PlaygroundBackend(slow_executor_factory)
    await backend.start()
    yield backend
    await backend.stop()


@pytest.fixture(scope="function")
def one_time_task() -> Task:
    return Task(
        name="One Time Task",
        schedule=OneTimeSchedule(execution_time=datetime.now(
            timezone.utc) + timedelta(minutes=5)),
        payload_schema_name="DummyPayload",
        payload=DummyPayload(message="One time task data").model_dump(),
        is_active=True
    )


@pytest.fixture(scope="function")
def recurring_task() -> Task:
    return Task(
        name="Recurring Task",
        schedule=RecurringSchedule(cron_expression="* * * * *"),
        payload_schema_name="DummyPayload",
        payload=DummyPayload(message="Recurring task data").model_dump(),
        is_active=True
    )


@pytest.fixture(scope="function")
def immediate_task() -> Task:
    return Task(
        name="Immediate Task",
        schedule=ImmediateSchedule(),
        payload_schema_name="DummyPayload",
        payload=DummyPayload(message="Immediate task data").model_dump(),
        is_active=True
    )


@pytest.mark.asyncio
async def test_create_task(backend: PlaygroundBackend, one_time_task: Task) -> None:
    task_id: str = await backend.create_task(one_time_task)
    assert task_id == one_time_task.id

    retrieved_task: Task | None = await backend.get_task(task_id)
    assert retrieved_task == one_time_task


@pytest.mark.asyncio
async def test_delay_task(backend: PlaygroundBackend, one_time_task: Task) -> None:
    # Set the execution time to 1 second from now
    one_time_task.schedule.execution_time = datetime.now(
        timezone.utc) + timedelta(seconds=1)
    print(f"Execution time: {one_time_task.schedule.execution_time}")

    task_id: str = await backend.create_task(one_time_task)

    # Check that the task is not executed immediately
    initial_job: Job | None = await backend.get_latest_job(task_id)
    assert initial_job is None

    # Wait for 4 seconds to ensure the task has been executed
    await asyncio.sleep(4)

    # Check that the task has been executed
    executed_job: Job | None = await backend.get_latest_job(task_id)
    assert executed_job is not None
    assert executed_job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_recurring_task(backend: PlaygroundBackend, recurring_task: Task) -> None:
    # Set the cron expression to run every second
    recurring_task.schedule.cron_expression = "* * * * * *"

    task_id: str = await backend.create_task(recurring_task)

    # Wait for 2.5 seconds to allow the task to run 2 times
    await asyncio.sleep(2.5)

    # Check that the task has been executed 2 times
    jobs: List[Job] = await backend.list_recent_jobs(task_id)
    assert len(jobs) == 2

    # Check that all jobs are completed
    for job in jobs:
        assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_auto_cancel_previous_job(slow_backend: PlaygroundBackend, recurring_task: Task) -> None:

    recurring_task.schedule.cron_expression = "* * * * * *"

    task_id: str = await slow_backend.create_task(recurring_task)

    # Wait for 3.5 seconds to allow 3 job executions to start
    await asyncio.sleep(3.5)

    # Check the jobs
    jobs: List[Job] = await slow_backend.list_recent_jobs(task_id)
    assert len(jobs) == 3, "Expected 3 jobs to be created"

    assert jobs[0].status == JobStatus.RUNNING, "Expected last job to be running"
    assert jobs[1].status == JobStatus.CANCELLED, "Expected second job to be cancelled"
    assert jobs[2].status == JobStatus.CANCELLED, "Expected first job to be cancelled"

    # Stop the backend
    await slow_backend.stop()

    jobs: List[Job] = await slow_backend.list_recent_jobs(task_id)
    assert jobs[2].status == JobStatus.CANCELLED, "Expected third job to be cancelled after backend is stopped"


@pytest.mark.asyncio
async def test_immediate_task(backend: PlaygroundBackend, immediate_task: Task) -> None:
    task_id: str = await backend.create_task(immediate_task)

    # Wait for a short time to allow the task to be executed
    await asyncio.sleep(0.1)

    # Check that the task has been executed immediately
    executed_job: Job | None = await backend.get_latest_job(task_id)
    assert executed_job is not None, "Expected immediate task to be executed"
    assert executed_job.status == JobStatus.COMPLETED, "Expected immediate task to be completed"

    # Check that only one job was created and executed
    jobs: List[Job] = await backend.list_recent_jobs(task_id)
    assert len(jobs) == 1, "Expected only one job for immediate task"
    assert jobs[0].status == JobStatus.COMPLETED, "Expected immediate task job to be completed"

@pytest.mark.asyncio
async def test_inactive_task(backend: PlaygroundBackend, one_time_task: Task) -> None:
    one_time_task.is_active = False
    task_id: str = await backend.create_task(one_time_task)

    # Set the execution time to now
    one_time_task.schedule.execution_time = datetime.now(timezone.utc)
    await backend.update_task(one_time_task)

    # Wait for a short time
    await asyncio.sleep(1)

    # Check that no job was created for the inactive task
    jobs: List[Job] = await backend.list_recent_jobs(task_id)
    assert len(jobs) == 0, "Expected no jobs for inactive task"

@pytest.mark.asyncio
async def test_activate_deactivate_task(backend: PlaygroundBackend, recurring_task: Task) -> None:
    recurring_task.schedule.cron_expression = "* * * * * *"
    task_id: str = await backend.create_task(recurring_task)

    # Wait for a short time to allow some jobs to be created
    await asyncio.sleep(2)

    # Deactivate the task
    await backend.deactivate_task(task_id)
    
    # Wait again
    await asyncio.sleep(2)

    # Check the number of jobs
    jobs_after_deactivation: List[Job] = await backend.list_recent_jobs(task_id)
    jobs_count_after_deactivation = len(jobs_after_deactivation)

    # Activate the task again
    await backend.activate_task(task_id)

    # Wait for more jobs to be created
    await asyncio.sleep(2)

    # Check that new jobs were created after reactivation
    jobs_after_reactivation: List[Job] = await backend.list_recent_jobs(task_id)
    assert len(jobs_after_reactivation) > jobs_count_after_deactivation, "Expected new jobs after reactivation"
