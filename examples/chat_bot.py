from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Type
from llm_scheduler.backends.playground import PlaygroundBackend
from llm_scheduler.executors.protocol import JobExecutor
from llm_scheduler.extractors.oai import OpenAIExtractor
from llm_scheduler.domain.task import Task
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.extractors.base import NoopResult

class AgentTask(BaseModel):
    action_command: str = Field(None, description="The action command to execute.")

class ChatMessage(BaseModel):
    message: str

class PrintExecutor(JobExecutor):
    @staticmethod
    def supported_schema() -> str:
        return "AgentTask"

    async def async_execute(self, job: Job) -> None:
        job.result = {
            "message": f"Executed job {job.id} with payload: {job.payload}"
        }
        print(f"Executing job {job.id} with payload: {job.payload}")

# Set up the backend and extractor
schemas: Dict[str, Type[BaseModel]] = {"AgentTask": AgentTask}
executor_factory = JobExecutorFactory(schemas)
executor_factory.register(PrintExecutor)
backend = PlaygroundBackend(executor_factory)
extractor = OpenAIExtractor(schemas)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await backend.start()
    yield
    # Shutdown
    await backend.stop()

app = FastAPI(lifespan=lifespan)

@app.post("/chat")
async def chat(message: ChatMessage):
    try:
        extract_result = await extractor.extract(message.message)
        if isinstance(extract_result, NoopResult):
            return {"response": "No task could be extracted from the input."}

        task = Task(
            name=message.message,
            schedule=extract_result.schedule,
            payload_schema_name=extract_result.payload_schema_name,
            payload=extract_result.payload
        )
        task_id = await backend.create_task(task)
        return {
            "response": f"{task.readable_string}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tasks")
async def get_tasks():
    tasks = await backend.list_tasks()
    return {"tasks": [task.model_dump() for task in tasks]}

@app.get("/tasks/{task_id}/jobs")
async def get_recent_jobs(task_id: str):
    jobs = await backend.list_recent_jobs(task_id)
    return {"jobs": [job.model_dump() for job in jobs]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
