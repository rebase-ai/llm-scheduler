from abc import ABC, abstractmethod
import asyncio
from typing import Any, Dict, List, Tuple, Type, Union

from pydantic import BaseModel

from llm_scheduler.domain.task import OneTimeSchedule, RecurringSchedule, ScheduleType


class ExtractResult(BaseModel):
    schedule: Union[OneTimeSchedule, RecurringSchedule]
    payload: Dict[str, Any]
    payload_schema_name: str
    payload_schema: Type[BaseModel]


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
        self.schemas = schemas

    @abstractmethod
    async def extract_schedule(self, input_data: Any) -> Union[OneTimeSchedule, RecurringSchedule]:
        """
        Extract schedule information from the input data.

        Args:
            input_data (Any): The input data to extract schedule information from.

        Returns:
            Union[OneTimeSchedule, RecurringSchedule]: The extracted schedule.

        Raises:
            ValueError: If the extracted schedule data is invalid.
        """
        pass

    @abstractmethod
    async def extract_payload(self, input_data: Any) -> Tuple[str, Dict[str, Any]]:
        """
        Extract payload information from the input data.

        Args:
            input_data (Any): The input data to extract payload information from.

        Returns:
            Tuple[str, Dict[str, Any]]: A tuple containing the schema name and the extracted payload conforming to the specified schema.

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

        schedule, (schema_name, payload) = await asyncio.gather(schedule_task, payload_task)

        schema = self.schemas[schema_name]

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
