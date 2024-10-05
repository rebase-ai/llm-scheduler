from typing import Dict, Type

from pydantic import BaseModel

from llm_scheduler.executors.protocol import JobExecutor


class JobExecutorFactory:
    """
    Factory class for creating job executors.
    """
    def __init__(self, schemas: Dict[str, Type[BaseModel]]):
        self._schemas: Dict[str, Type[BaseModel]] = schemas
        self._executors: Dict[str, Type[JobExecutor]] = {}

    @property
    def supported_schemas(self) -> Dict[str, Type[BaseModel]]:
        return self._schemas

    def register(self, executor_class: Type[JobExecutor]) -> None:
        """
        Register a new executor class with its supported schema.

        Args:
            executor_class (Type[JobExecutor]): The executor class to register.
        """
        schema_name: str = executor_class.supported_schema()
        if schema_name not in self._schemas:
            raise ValueError(f"Schema '{schema_name}' is not supported")
        if schema_name in self._executors:
            raise ValueError(f"An executor for schema '{schema_name}' is already registered")
        self._executors[schema_name] = executor_class

    def get_executor(self, schema_name: str, payload: dict) -> JobExecutor:
        """
        Get an executor instance by schema name and validate the payload.

        Args:
            schema_name (str): The name of the schema to use.
            payload (Dict[str, Any]): The payload to validate and use.

        Returns:
            JobExecutor: An instance of the appropriate executor.

        Raises:
            KeyError: If the requested schema name is not registered.
            ValueError: If the payload is invalid for the given schema.
        """
        if schema_name not in self._schemas:
            raise KeyError(f"No schema registered with name '{schema_name}'")
        if schema_name not in self._executors:
            raise KeyError(f"No executor registered for schema '{schema_name}'")

        schema_class = self._schemas[schema_name]
        try:
            validated_payload = schema_class(**payload)
        except ValueError as e:
            raise ValueError(f"Invalid payload for schema '{schema_name}': {str(e)}")

        executor_class = self._executors[schema_name]
        return executor_class()
