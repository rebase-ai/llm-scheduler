import pytest
from typing import Dict, Type
from pydantic import BaseModel
from llm_scheduler.executor_factory import JobExecutor, JobExecutorFactory
from llm_scheduler.domain.job import Job

class DummyPayload(BaseModel):
    message: str

class DummyExecutor(JobExecutor):
    @staticmethod
    def supported_schema() -> str:
        return "DummyPayload"

    def execute(self, job: Job) -> None:
        pass

    async def async_execute(self, job: Job) -> None:
        pass

@pytest.fixture
def schemas() -> Dict[str, Type[BaseModel]]:
    return {"DummyPayload": DummyPayload}

@pytest.fixture
def factory(schemas: Dict[str, Type[BaseModel]]) -> JobExecutorFactory:
    return JobExecutorFactory(schemas)

def test_register_executor(factory: JobExecutorFactory) -> None:
    factory.register(DummyExecutor)
    assert "DummyPayload" in factory._executors

def test_register_executor_invalid_schema(factory: JobExecutorFactory) -> None:
    class InvalidExecutor(JobExecutor):
        @staticmethod
        def supported_schema() -> str:
            return "InvalidSchema"

        def execute(self, job: Job) -> None:
            pass

        async def async_execute(self, job: Job) -> None:
            pass

    with pytest.raises(ValueError, match="Schema 'InvalidSchema' is not supported"):
        factory.register(InvalidExecutor)

def test_register_executor_duplicate(factory: JobExecutorFactory) -> None:
    factory.register(DummyExecutor)
    with pytest.raises(ValueError, match="An executor for schema 'DummyPayload' is already registered"):
        factory.register(DummyExecutor)

def test_get_executor(factory: JobExecutorFactory) -> None:
    factory.register(DummyExecutor)
    payload = DummyPayload(message="test")
    executor = factory.get_executor("DummyPayload", payload.model_dump())
    assert isinstance(executor, DummyExecutor)

def test_get_executor_invalid_schema(factory: JobExecutorFactory) -> None:
    with pytest.raises(KeyError, match="No schema registered with name 'InvalidSchema'"):
        factory.get_executor("InvalidSchema", DummyPayload(message="test").model_dump())

def test_get_executor_unregistered(factory: JobExecutorFactory) -> None:
    with pytest.raises(KeyError, match="No executor registered for schema 'DummyPayload'"):
        factory.get_executor("DummyPayload", DummyPayload(message="test").model_dump())

def test_get_executor_invalid_payload(factory: JobExecutorFactory) -> None:
    factory.register(DummyExecutor)
    
    class InvalidPayload(BaseModel):
        invalid_field: str

    with pytest.raises(ValueError, match="Invalid payload for schema 'DummyPayload'"):
        factory.get_executor("DummyPayload", InvalidPayload(invalid_field="test").model_dump())
