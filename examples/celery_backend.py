import asyncio
from typing import Dict, Type
from celery import Celery
from pydantic import BaseModel, Field
from llm_scheduler.backends.celery.backend import CeleryBackend
from llm_scheduler.executors.protocol import JobExecutor
from llm_scheduler.extractors.oai import OpenAIExtractor
from llm_scheduler.domain.task import Task
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.storages.sqlalchemy import InMemoryStorage, SqlAlchemyStorage

class AgentTask(BaseModel):
    intent: str = Field(..., description="The user's intent or command")

class PrintExecutor(JobExecutor):
    @staticmethod
    def supported_schema() -> str:
        return "AgentTask"

    async def async_execute(self, job: Job) -> None:
        print(f"Executing job {job.id} with payload: {job.payload}")
# Set up the backend and extractor
schemas = {"AgentTask": AgentTask}
executor_factory = JobExecutorFactory(schemas)
executor_factory.register(PrintExecutor)

celery_app = Celery('llm_scheduler_app', broker='redis://localhost:6379/2', backend='redis://localhost:6379/3')
celery_app.conf.update(
    task_always_eager=False,  # Tasks will be executed immediately
    task_eager_propagates=False,
    beat_scheduler='redbeat.RedBeatScheduler',
    redbeat_redis_url='redis://localhost:6379/1'
)

storage = SqlAlchemyStorage(
    db_url="sqlite+aiosqlite:///./task.db"
)

backend = CeleryBackend(executor_factory, storage, celery_app)
extractor = OpenAIExtractor(schemas)

async def get_user_input():
    return await asyncio.to_thread(input, "> ")

async def chatbot():
    print("Welcome to the chatbot! Type 'exit' to quit.")
    while True:
        user_input = await get_user_input()
        if user_input.lower() == "exit":
            break

        try:
            schedule = await extractor.extract_schedule(user_input)
            schema_name, payload = await extractor.extract_payload(user_input)
            task = Task(
                name=f"User Intent: {payload['intent']}",
                schedule=schedule,
                payload_schema_name=schema_name,
                payload=payload
            )
            await backend.create_task(task)
            print(f"Task created: {task.name}")
            print(f"Schedule: {schedule}")
        except Exception as e:
            print(f"Error: {e}")

async def main():
    await storage.create_tables()
    await backend.start()
    await chatbot()
    await backend.stop()

if __name__ == "__main__":

    # create worker in other thread
    import threading
    def start_worker():
        import os
        os.system("celery -A examples.celery_backend.celery_app worker --beat --scheduler redbeat.RedBeatScheduler -P solo --loglevel=info")

    worker_thread = threading.Thread(target=start_worker)
    worker_thread.start()

    asyncio.run(main())

    worker_thread.join()
