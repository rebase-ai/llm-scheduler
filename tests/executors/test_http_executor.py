import pytest
from aioresponses import aioresponses
from llm_scheduler.executors.http import HttpJobExecutor, HttpCallPayload
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.domain.task import Task, OneTimeSchedule
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

@pytest.fixture(scope="function")
def http_executor():
    return HttpJobExecutor()

@pytest.fixture(scope="function")
def sample_job():
    task = Task(
        name="Test HTTP Task",
        schedule=OneTimeSchedule(execution_time=datetime.now(ZoneInfo("UTC")) + timedelta(minutes=5)),
        payload=HttpCallPayload(
            method="GET",
            url="https://example.com",
            headers={"Content-Type": "application/json"},
            params={"key": "value"}
        ).model_dump(),
        payload_schema_name="HttpCallPayload"
    )
    return Job(task=task)

@pytest.mark.asyncio
async def test_async_execute_success(http_executor, sample_job):
    with aioresponses() as m:
        m.get(
            'https://example.com?key=value',
            status=200,
            headers={"Content-Type": "application/json"},
            body='{"result": "success"}'
        )

        await http_executor.async_execute(sample_job)

        assert sample_job.status == JobStatus.COMPLETED
        assert sample_job.result == {
            "status": 200,
            "headers": {"Content-Type": "application/json"},
            "body": '{"result": "success"}'
        }

@pytest.mark.asyncio
async def test_async_execute_failure(http_executor, sample_job):
    sample_job.task.payload["url"] = "https://non-existent-url.com"

    with aioresponses() as m:
        m.get(
            'https://non-existent-url.com?key=value',
            exception=Exception("Connection error")
        )

        await http_executor.async_execute(sample_job)

        assert sample_job.status == JobStatus.FAILED
        assert "error" in sample_job.result
        assert "Unexpected error: Connection error" in sample_job.result["error"]

@pytest.mark.asyncio
async def test_async_execute_invalid_payload(http_executor, sample_job):
    sample_job.task.payload = {"invalid": "payload"}

    await http_executor.async_execute(sample_job)

    assert sample_job.status == JobStatus.FAILED
    assert "error" in sample_job.result
    assert "Invalid payload" in sample_job.result["error"]
