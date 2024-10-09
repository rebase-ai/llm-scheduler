import asyncio
from typing import Dict, Type
from celery import Celery
from pydantic import BaseModel, Field
from llm_scheduler.backends.celery import CeleryBackend
from llm_scheduler.executors.protocol import JobExecutor
from llm_scheduler.extractors.oai import OpenAIExtractor
from llm_scheduler.domain.task import Task
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.executor_factory import JobExecutorFactory
from llm_scheduler.storages.sqlalchemy import SqlAlchemyStorage
from llm_scheduler.extractors.base import NoopResult

class AgentTask(BaseModel):
    action_command: str = Field(None, description="The action command to execute.")

class PrintExecutor(JobExecutor):
    @staticmethod
    def supported_schema() -> str:
        return "AgentTask"

    async def async_execute(self, job: Job) -> None:
        print(f"Executing job {job.id} with payload: {job.payload}")

# Set up the backend and extractor
schemas: Dict[str, Type[BaseModel]] = {"AgentTask": AgentTask}
executor_factory = JobExecutorFactory(schemas)
executor_factory.register(PrintExecutor)

celery_app = Celery('llm_scheduler_app', broker='redis://localhost:6379/2', backend='redis://localhost:6379/3')
celery_app.conf.update(
    task_always_eager=False,
    task_eager_propagates=False,
    beat_scheduler='redbeat.RedBeatScheduler',
    redbeat_redis_url='redis://localhost:6379/1'
)

storage = SqlAlchemyStorage(
    db_url="sqlite+aiosqlite:///./task.db"
)

backend = CeleryBackend(executor_factory, storage, celery_app)
extractor = OpenAIExtractor(schemas)

async def get_user_input() -> str:
    return await asyncio.to_thread(input, "> ")

async def chatbot() -> None:
    print("Welcome to the chatbot! Type 'exit' to quit.")
    while True:
        user_input = await get_user_input()
        if user_input.lower() == "exit":
            break

        try:
            extract_result = await extractor.extract(user_input)
            if isinstance(extract_result, NoopResult):
                print("No task could be extracted from the input.")
                continue

            task = Task(
                name=user_input,  # Use the original user input as the task name
                schedule=extract_result.schedule,
                payload_schema_name=extract_result.payload_schema_name,
                payload=extract_result.payload
            )
            await backend.create_task(task)
            print(f"Task created: {task.name}")
            print(f"Schedule: {task.schedule}")
        except Exception as e:
            print(f"Error: {e}")

async def main() -> None:
    await storage.create_tables()
    await backend.start()
    await chatbot()
    await backend.stop()

if __name__ == "__main__":
    # create worker in other thread
    import threading
    def start_worker() -> None:
        import os
        os.system("celery -A examples.celery_backend.celery_app worker --beat --scheduler redbeat.RedBeatScheduler -P solo --loglevel=info")

    worker_thread = threading.Thread(target=start_worker)
    worker_thread.start()

    asyncio.run(main())

    worker_thread.join()
