import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from llm_scheduler.storages.sqlalchemy import InMemoryStorage, SqlAlchemyStorage, Base
from llm_scheduler.domain.task import Task, OneTimeSchedule, ScheduleType
from llm_scheduler.domain.job import Job, JobStatus
from datetime import datetime, timezone


@pytest_asyncio.fixture(scope="module")
async def sqlite_storage():
    storage = InMemoryStorage()
    await storage.create_tables()
    yield storage


@pytest.mark.asyncio
async def test_create_and_get_task(sqlite_storage: SqlAlchemyStorage):
    task = Task(
        id="test_task_1",
        name="Test Task",
        description="A test task",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema",
        meta={"meta_key": "meta_value"},
        is_active=True
    )

    # Create task
    task_id = await sqlite_storage.create_task(task)
    assert task_id == "test_task_1"

    # Get task
    retrieved_task = await sqlite_storage.get_task(task_id)
    assert retrieved_task is not None
    assert retrieved_task.id == task.id
    assert retrieved_task.name == task.name
    assert retrieved_task.description == task.description
    assert retrieved_task.payload == task.payload
    assert retrieved_task.payload_schema_name == task.payload_schema_name
    assert retrieved_task.meta == task.meta
    assert retrieved_task.is_active == task.is_active


@pytest.mark.asyncio
async def test_create_and_get_job(sqlite_storage: SqlAlchemyStorage):
    # First, create a task
    task = Task(
        id="test_task_2",
        name="Test Task for Job",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema"
    )
    await sqlite_storage.create_task(task)

    # Create job
    job = Job(
        id="test_job_1",
        task=task,
        status=JobStatus.PENDING,
        result=None
    )

    job_id = await sqlite_storage.create_job(job)
    assert job_id == "test_job_1"

    # Get job
    retrieved_job = await sqlite_storage.get_job(job_id)
    assert retrieved_job is not None
    assert retrieved_job.id == job.id
    assert retrieved_job.task.id == task.id
    assert retrieved_job.status == job.status
    assert retrieved_job.result == job.result


@pytest.mark.asyncio
async def test_update_task(sqlite_storage: SqlAlchemyStorage):
    # First, create a task
    task = Task(
        id="test_task_3",
        name="Test Task for Update",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema"
    )
    await sqlite_storage.create_task(task)

    # Update task
    task.name = "Updated Test Task"
    task.description = "This is an updated description"
    task.payload = {"new_key": "new_value"}
    task.is_active = False

    update_success = await sqlite_storage.update_task(task)
    assert update_success is True

    # Get updated task
    updated_task = await sqlite_storage.get_task(task.id)
    assert updated_task is not None
    assert updated_task.name == "Updated Test Task"
    assert updated_task.description == "This is an updated description"
    assert updated_task.payload == {"new_key": "new_value"}
    assert updated_task.is_active is False


@pytest.mark.asyncio
async def test_delete_task(sqlite_storage: SqlAlchemyStorage):
    # First, create a task
    task = Task(
        id="test_task_4",
        name="Test Task for Deletion",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema"
    )
    await sqlite_storage.create_task(task)

    # Delete task
    delete_success = await sqlite_storage.delete_task(task.id)
    assert delete_success is True

    # Try to get deleted task
    deleted_task = await sqlite_storage.get_task(task.id)
    assert deleted_task is None


@pytest.mark.asyncio
async def test_list_tasks(sqlite_storage: SqlAlchemyStorage):
    # Create multiple tasks
    tasks = [
        Task(
            id=f"list_task_{i}",
            name=f"List Task {i}",
            created_at=datetime.now(timezone.utc),
            schedule=OneTimeSchedule(
                execution_time=datetime.now(timezone.utc)),
            payload={"key": f"value_{i}"},
            payload_schema_name="test_schema"
        ) for i in range(5)
    ]

    for task in tasks:
        await sqlite_storage.create_task(task)

    # List tasks
    listed_tasks = await sqlite_storage.list_tasks(limit=3, offset=0)
    assert len(listed_tasks) == 3
    assert listed_tasks[0].id == "list_task_4"
    assert listed_tasks[2].id == "list_task_2"


@pytest.mark.asyncio
async def test_update_job(sqlite_storage: SqlAlchemyStorage):
    # First, create a task and a job
    task = Task(
        id="test_task_5",
        name="Test Task for Job Update",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema"
    )
    await sqlite_storage.create_task(task)

    job = Job(
        id="test_job_2",
        task=task,
        status=JobStatus.PENDING,
        result=None
    )
    await sqlite_storage.create_job(job)

    # Update job
    job.status = JobStatus.COMPLETED
    job.result = {"output": "success"}

    update_success = await sqlite_storage.update_job(job)
    assert update_success is True

    # Get updated job
    updated_job = await sqlite_storage.get_job(job.id)
    assert updated_job is not None
    assert updated_job.status == JobStatus.COMPLETED
    assert updated_job.result == {"output": "success"}


@pytest.mark.asyncio
async def test_list_jobs(sqlite_storage: SqlAlchemyStorage):
    # First, create a task
    task = Task(
        id="test_task_6",
        name="Test Task for Job Listing",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema"
    )
    await sqlite_storage.create_task(task)

    # Create multiple jobs
    jobs = [
        Job(
            id=f"list_job_{i}",
            task=task,
            status=JobStatus.PENDING,
            result=None
        ) for i in range(5)
    ]

    for job in jobs:
        await sqlite_storage.create_job(job)

    # List jobs
    listed_jobs = await sqlite_storage.list_recent_jobs(task.id, limit=3)
    assert len(listed_jobs) == 3
    assert all(job.task.id == task.id for job in listed_jobs)


@pytest.mark.asyncio
async def test_get_recent_job(sqlite_storage: SqlAlchemyStorage):
    # First, create a task
    task = Task(
        id="test_task_7",
        name="Test Task for Recent Job",
        created_at=datetime.now(timezone.utc),
        schedule=OneTimeSchedule(execution_time=datetime.now(timezone.utc)),
        payload={"key": "value"},
        payload_schema_name="test_schema"
    )
    await sqlite_storage.create_task(task)

    # Create multiple jobs
    jobs = [
        Job(
            id=f"recent_job_{i}",
            task=task,
            status=JobStatus.PENDING,
            result=None
        ) for i in range(3)
    ]

    for job in jobs:
        await sqlite_storage.create_job(job)

    # Get recent job
    recent_job = await sqlite_storage.get_recent_job(task.id)
    assert recent_job is not None
    assert recent_job.id == "recent_job_2"  # The last created job
