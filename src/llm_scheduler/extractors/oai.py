from typing import Any, Dict, Optional, Callable, List, Tuple, Type, Union
import json
from pydantic import BaseModel, Field, ValidationError
from llm_scheduler.extractors.base import BaseExtractor, ExtractResult, NoopResult
try:
    from langfuse.openai import AsyncOpenAI
except ImportError:
    from openai import AsyncOpenAI
import os
import tzlocal
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from llm_scheduler.domain.task import BaseSchedule, OneTimeSchedule, RecurringSchedule, ImmediateSchedule, ScheduleType


class ImmediateScheduleSchema(BaseModel):
    description: Optional[str] = Field(None, description="Description for an immediate task execution.")

    def to_schedule(self) -> ImmediateSchedule:
        return ImmediateSchedule(description=self.description)

class OneTimeScheduleSchema(BaseModel):
    execution_time: str = Field(..., description="ISO formatted datetime string with timezone information for a one-time delayed task execution. This field represents when the task should be executed.")
    description: Optional[str] = Field(None, description="Original schedule description for a one-time delayed task. This field can store raw content extracted by LLM, including timezone info and other descriptive information about when and how the task should be executed.")

    def to_schedule(self) -> OneTimeSchedule:
        execution_time = datetime.fromisoformat(self.execution_time)
        if execution_time.tzinfo is None:
            execution_time = execution_time.replace(tzinfo=ZoneInfo("UTC"))
        return OneTimeSchedule(
            execution_time=execution_time,
            description=self.description
        )

class RecurringScheduleSchema(BaseModel):
    cron_expression: str = Field(
        ...,
        description="Cron expression defining the recurring execution pattern for a periodic task. Please preserve the original timezone information from the natural language description when generating this expression.",
    )
    start_time: Optional[str] = Field(None, description="ISO formatted datetime string with timezone information for the start time of a recurring task. This field indicates when the periodic execution should begin.")
    end_time: Optional[str] = Field(None, description="ISO formatted datetime string with timezone information for the end time of a recurring task. This field indicates when the periodic execution should cease.")
    description: Optional[str] = Field(None, description="Original schedule description for a recurring task. This field can store raw content extracted by LLM, including timezone info and other descriptive information about the frequency and timing of task execution.")
    timezone: str = Field(default="UTC", description="Timezone information for the recurring task. If not specified, default to UTC.")

    def to_schedule(self) -> RecurringSchedule:
        timezone = ZoneInfo(self.timezone)
        
        def localize_time(time_str: Optional[str]) -> Optional[datetime]:
            if time_str:
                dt = datetime.fromisoformat(time_str)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone)
                return dt.astimezone(timezone)
            return None

        cron_parts = self.cron_expression.split()
        if len(cron_parts) == 5:
            minute, hour, day, month, day_of_week = cron_parts
            if hour.isdigit():
                utc_hour = int((int(hour) - timezone.utcoffset(datetime.now()).total_seconds() // 3600) % 24)
                utc_cron = f"{minute} {utc_hour} {day} {month} {day_of_week}"
            else:
                utc_cron = self.cron_expression
        else:
            utc_cron = self.cron_expression

        return RecurringSchedule(
            cron_expression=utc_cron,
            start_time=localize_time(self.start_time),
            end_time=localize_time(self.end_time),
            description=self.description,
            timezone=timezone
        )


DEFAULT_EXTRACT_SCHEDULE_SYSTEM_PROMPT: str = """You are a schedule extractor. Your task is to carefully analyze the given input and extract schedule information according to the provided schemas. Pay close attention to the schema descriptions and ensure that all extracted information conforms to the specified formats.

Important: If the user's instruction doesn't specify a particular execution time or recurrence pattern, assume the task should be executed immediately.

current time: {current_time}

{schema_descriptions}

Return a JSON object with the following structure:
{{
    "schema_name": "Name of the matched schema",
    "schedule": {{
        // Extracted schedule details according to the chosen schema
    }}
}}

Ensure that all extracted information is accurate and complete based on the input provided."""

DEFAULT_EXTRACT_PAYLOAD_SYSTEM_PROMPT: str = """You are a payload extractor. Your task is to meticulously analyze the given input and extract payload information according to the provided schemas. Pay careful attention to the schema descriptions and ensure that all extracted information strictly adheres to the specified formats and data types.

{schema_descriptions}

Return a JSON object with the following structure:
{{
    "schema_name": "Name of the matched schema",
    "payload": {{
        // Extracted payload details according to the chosen schema
    }}
}}

If multiple schemas could potentially match the input, choose the most appropriate one based on the available information. Ensure that all extracted information is accurate, complete, and fully compliant with the chosen schema's requirements.

If no task or actionable intent can be extracted from the input, use the __BuiltInNoopTask__ schema and provide a reason for why no task could be identified."""

class OpenAIExtractor(BaseExtractor):
    def __init__(
        self, 
        schemas: Dict[str, Type[BaseModel]], 
        api_key: Optional[str] = None, 
        model: str = "gpt-4o",
        extract_schedule_prompt: str = DEFAULT_EXTRACT_SCHEDULE_SYSTEM_PROMPT,
        extract_payload_prompt: str = DEFAULT_EXTRACT_PAYLOAD_SYSTEM_PROMPT,
        transform: Optional[Callable[[Any], Any]] = None,
        system_message: Optional[str] = None
    ):
        super().__init__(schemas)
        if api_key is None or api_key == "":
            api_key = os.environ.get("OPENAI_API_KEY")
            if api_key is None:
                raise ValueError("OpenAI API key not provided and not found in environment variables.")
        self.client: AsyncOpenAI = AsyncOpenAI(api_key=api_key)
        self.model: str = model 
        self.extract_schedule_prompt: str = extract_schedule_prompt
        self.extract_payload_prompt: str = extract_payload_prompt
        self.transform: Optional[Callable[[Any], Any]] = transform
        self.system_message: Optional[str] = system_message
        self.schema_descriptions: str = self._generate_schema_descriptions(schemas)
        self.schedule_schema_descriptions: str = self._generate_schema_descriptions({
            "ImmediateScheduleSchema": ImmediateScheduleSchema,
            "OneTimeScheduleSchema": OneTimeScheduleSchema,
            "RecurringScheduleSchema": RecurringScheduleSchema
        })

    def _generate_schema_descriptions(self, schemas: Dict[str, Type[BaseModel]]) -> str:
        """
        Generate a string containing descriptions of all schemas with enhanced formatting.
        """
        descriptions: List[str] = []
        for schema_name, schema in schemas.items():
            schema_description: str = f"### Schema: {schema_name}\n"
            schema_description += "Fields:\n"
            for field_name, field in schema.model_fields.items():
                field_description: str = f"  - Field: {field_name}\n"
                field_description += f"    Description: {field.description}\n"
                field_description += f"    Type: {field.annotation}\n"
                schema_description += field_description
            schema_description += "---\n"  # Add a separator between schemas
            descriptions.append(schema_description)
        return "\n".join(descriptions)

    async def extract_schedule(self, input_data: Any) -> Optional[Union[ImmediateSchedule, OneTimeSchedule, RecurringSchedule]]:
        """
        Extract schedule information from the input data.
        """
        prompt: str = self.extract_schedule_prompt.format(schema_descriptions=self.schedule_schema_descriptions, current_time=datetime.now(tzlocal.get_localzone()).isoformat())
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": str(input_data)}
            ],
            response_format={ "type": "json_object" }
        )
        result: Dict[str, Any] = json.loads(response.choices[0].message.content)
        schema_name: str = result["schema_name"]
        schedule_data: Dict[str, Any] = result["schedule"]

        if schema_name == "ImmediateScheduleSchema":
            schema: Type[BaseModel] = ImmediateScheduleSchema
        elif schema_name == "OneTimeScheduleSchema":
            schema: Type[BaseModel] = OneTimeScheduleSchema
        elif schema_name == "RecurringScheduleSchema":
            schema: Type[BaseModel] = RecurringScheduleSchema
        else:
            return None

        try:
            validated_data: Union[ImmediateScheduleSchema, OneTimeScheduleSchema, RecurringScheduleSchema] = schema(**schedule_data)
            return validated_data.to_schedule()
        except ValidationError:
            return None

    async def extract_payload(self, input_data: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Extract payload information from the input data.
        """
        prompt: str = self.extract_payload_prompt.format(schema_descriptions=self.schema_descriptions)
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": str(input_data)}
            ],
            response_format={ "type": "json_object" }
        )
        result: Dict[str, Any] = json.loads(response.choices[0].message.content)
        schema_name: str = result["schema_name"]
        payload_data: Dict[str, Any] = result["payload"]

        if schema_name not in self.schemas:
            return None

        schema: Type[BaseModel] = self.schemas[schema_name]
        try:
            validated_data: BaseModel = schema(**payload_data)
            return schema_name, validated_data.model_dump()
        except ValidationError:
            return None

