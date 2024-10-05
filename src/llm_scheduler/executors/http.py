import asyncio
from typing import Any, Dict, Optional

import aiohttp
from pydantic import BaseModel, Field

from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.executors.protocol import JobExecutor


class HttpCallPayload(BaseModel):
    url: str = Field(..., description="The URL to make the HTTP request to")
    method: str = Field(..., description="The HTTP method to use (e.g. GET, POST, PUT, DELETE)")
    headers: Dict[str, str] = Field(default={}, description="Optional headers to include in the request")
    body: Dict[str, Any] = Field(default={}, description="Optional body payload for the request")
    params: Dict[str, str] = Field(default={}, description="Optional query parameters for the request")

class HttpJobExecutor(JobExecutor):
    """
    Job executor for making HTTP requests using aiohttp.
    """

    @staticmethod
    def supported_schema() -> str:
        return HttpCallPayload.model_json_schema()['title']
    
    async def async_execute(self, job: Job) -> None:
        """
        Asynchronously execute the given job by making an HTTP request.

        Args:
            job (Job): The job to be executed.
        """
        job.set_status(JobStatus.RUNNING)

        try:
            payload = HttpCallPayload.model_validate(job.payload)
        except ValueError as e:
            job.set_result({"error": f"Invalid payload: {str(e)}"}, status=JobStatus.FAILED)
            return

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=payload.method,
                    url=payload.url,
                    headers=payload.headers,
                    params=payload.params,
                    json=payload.body
                ) as response:
                    result: Dict[str, Any] = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "body": await response.text()
                    }
                    job.set_result(result, status=JobStatus.COMPLETED)
        except Exception as e:
            job.set_result({"error": f"Unexpected error: {str(e)}"}, status=JobStatus.FAILED)

    def execute(self, job: Job) -> None:
        """
        Synchronous version of execute method.
        """
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_execute(job))
