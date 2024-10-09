from abc import ABC, abstractmethod
import asyncio
from typing import Any, Dict, List, Tuple, Type, Union, Optional

from pydantic import BaseModel, Field

from llm_scheduler.domain.task import ImmediateSchedule, OneTimeSchedule, RecurringSchedule, ScheduleType


class NoopTask(BaseModel):
    '''
    built-in schema for returning a noop task
    '''
    reason: str = Field(..., description="The reason for the noop task.")

class ExtractResult(BaseModel):
    """
    Represents the result of extracting schedule and payload information.

    Attributes:
        schedule (Optional[Union[ImmediateSchedule, OneTimeSchedule, RecurringSchedule]]): The extracted schedule information.
            If None, it indicates that no valid task schedule was found or extracted.
        payload (Optional[Dict[str, Any]]): The extracted payload information.
        payload_schema_name (Optional[str]): The name of the schema used for the payload.
        payload_schema (Optional[Type[BaseModel]]): The Pydantic model class for the payload schema.
    """
    schedule: Optional[Union[ImmediateSchedule, OneTimeSchedule, RecurringSchedule]] = Field(None, description="Extracted schedule information")
    payload: Optional[Dict[str, Any]] = Field(None, description="Extracted payload information")
    payload_schema_name: Optional[str] = Field(None, description="Name of the schema used for the payload")
    payload_schema: Optional[Type[BaseModel]] = Field(None, description="Pydantic model class for the payload schema")


class NoopResult(ExtractResult):
    """
    Represents a result where no valid schedule or payload could be extracted.
    """
    def __init__(self):
        super().__init__(schedule=None, payload=None, payload_schema_name=None, payload_schema=None)

class BaseExtractor(ABC):
    """
    Abstract base class for task extractors.
    """

    @abstractmethod
    def __init__(self, schemas: Dict[str, Type[BaseModel]]) -> None:
        """
        Initialize the extractor with a dictionary of allowed schemas.

        Args:
            schemas (Dict[str, Type[BaseModel]]): A dictionary mapping schema names to their corresponding Pydantic models.
        """
        self.schemas = schemas.copy()
        self.schemas["__BuiltInNoopTask__"] = NoopTask


    @abstractmethod
    async def extract_schedule(self, input_data: Any) -> Optional[Union[ImmediateSchedule, OneTimeSchedule, RecurringSchedule]]:
        """
        Extract schedule information from the input data.

        Args:
            input_data (Any): The input data to extract schedule information from.

        Returns:
            Optional[Union[ImmediateSchedule, OneTimeSchedule, RecurringSchedule]]: The extracted schedule, or None if no valid schedule could be extracted.

        Raises:
            ValueError: If the extracted schedule data is invalid.
        """
        pass

    @abstractmethod
    async def extract_payload(self, input_data: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Extract payload information from the input data.

        Args:
            input_data (Any): The input data to extract payload information from.

        Returns:
            Optional[Tuple[str, Dict[str, Any]]]: A tuple containing the schema name and the extracted payload conforming to the specified schema, or None if no valid payload could be extracted.

        Raises:
            ValueError: If the extracted payload data does not conform to any of the allowed schemas.
        """
        pass

    async def extract(self, input_data: Any) -> ExtractResult:
        """
        Extract task information from the input data and produce a valid payload.

        Args:
            input_data (Any): The input data to extract task information from.

        Returns:
            ExtractResult: An ExtractResult object containing the extracted schedule, payload, schema name, and schema.

        Raises:
            ValueError: If the extracted data does not conform to any of the allowed schemas.
        """
        schedule_task = asyncio.create_task(self.extract_schedule(input_data))
        payload_task = asyncio.create_task(self.extract_payload(input_data))

        schedule, payload_data = await asyncio.gather(schedule_task, payload_task)
        if payload_data is None or payload_data[0] == "__BuiltInNoopTask__":
            return NoopResult()

        schema_name, payload = payload_data
        if schema_name not in self.schemas:
            raise ValueError(
                f"Schema '{schema_name}' not found in allowed schemas.")
        schema = self.schemas[schema_name]

        if schedule is None:
            schedule = ImmediateSchedule()

        return ExtractResult(
            schedule=schedule,
            payload=payload,
            payload_schema_name=schema_name,
            payload_schema=schema
        )

    def get_allowed_schemas(self) -> List[str]:
        """
        Get the list of allowed schema names.

        Returns:
            List[str]: A list of allowed schema names.
        """
        return list(self.schemas.keys())
