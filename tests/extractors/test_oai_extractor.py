import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Type

import pytest
from pydantic import BaseModel, Field

from llm_scheduler.extractors.oai import (
    OpenAIExtractor,
)
from llm_scheduler.domain.task import OneTimeSchedule, RecurringSchedule


class HttpCall(BaseModel):
    url: str = Field(..., description="The URL to make the HTTP request to")
    method: str = Field(..., description="The HTTP method to use (e.g. GET, POST, PUT, DELETE)")
    headers: Dict[str, str] = Field(default={}, description="Optional headers to include in the request")
    body: Dict[str, Any] = Field(default={}, description="Optional body payload for the request")
    params: Dict[str, str] = Field(default={}, description="Optional query parameters for the request")

class RunPythonCode(BaseModel):
    code: str = Field(..., description="The Python code to execute")
    packages: List[str] = Field(default=[], description="Optional list of packages to install before running the code")
    timeout: int = Field(default=10, description="Optional timeout in seconds for code execution")


@pytest.fixture
def openai_extractor() -> OpenAIExtractor:
    """
    Fixture to create an instance of OpenAIExtractor with real OpenAI API key.
    """
    schemas: Dict[str, Type[BaseModel]] = {
        "HttpCall": HttpCall,
        "RunPythonCode": RunPythonCode,
    }
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OpenAI API key is not set in environment variables.")
    
    return OpenAIExtractor(schemas=schemas, api_key=api_key)

@pytest.mark.asyncio
async def test_extract_one_time_schedule(openai_extractor: OpenAIExtractor):
    """
    Test the extract_schedule method for a one-time schedule using real OpenAI API.
    """
    input_data = "Schedule a meeting on 2023-10-15 at 10:00 AM UTC."
    
    schedule = await openai_extractor.extract_schedule(input_data)
    
    expected_time = datetime(2023, 10, 15, 10, 0, 0, tzinfo=timezone.utc)
    assert isinstance(schedule, OneTimeSchedule)
    assert schedule.execution_time == expected_time

@pytest.mark.asyncio
async def test_extract_recurring_schedule(openai_extractor: OpenAIExtractor):
    """
    Test the extract_schedule method for a recurring schedule using real OpenAI API.
    """
    input_data = "Schedule a weekly meeting every Monday at 9:00 AM UTC."
    
    schedule = await openai_extractor.extract_schedule(input_data)
    
    assert isinstance(schedule, RecurringSchedule)
    assert schedule.cron_expression in ["0 9 * * 1", "0 9 * * MON"]

@pytest.mark.asyncio
async def test_extract_http_call_payload(openai_extractor: OpenAIExtractor):
    """
    Test the extract_payload method for an HTTP call using real OpenAI API.
    """
    input_data = "Call the /api/v1/users endpoint with a GET request and the following parameters: {'name': 'John', 'age': 30}"
    
    schema_name, payload_dict = await openai_extractor.extract_payload(input_data)
    
    assert schema_name == "HttpCall"
    assert payload_dict["url"] == "/api/v1/users"
    assert payload_dict["method"] == "GET"
    assert payload_dict["params"] == {"name": "John", "age": "30"}
    assert isinstance(payload_dict, dict)

@pytest.mark.asyncio
async def test_extract_run_python_code_payload(openai_extractor: OpenAIExtractor):
    """
    Test the extract_payload method for running Python code using real OpenAI API.
    """
    input_data = "Run the following Python code: print('Hello, World!') with a timeout of 5 seconds."
    
    schema_name, payload_dict = await openai_extractor.extract_payload(input_data)
    
    assert schema_name == "RunPythonCode"
    assert payload_dict["code"] == "print('Hello, World!')"
    assert payload_dict["timeout"] == 5
    assert isinstance(payload_dict, dict)

@pytest.mark.asyncio
async def test_extract_complex_http_call_payload(openai_extractor: OpenAIExtractor):
    """
    Test the extract_payload method for a complex HTTP call using real OpenAI API.
    """
    input_data = """
    Make a POST request to https://api.example.com/users with the following:
    Headers: 
    - Content-Type: application/json
    - Authorization: Bearer token123
    Body:
    {
        "name": "Alice",
        "email": "alice@example.com",
        "age": 28
    }
    """
    
    schema_name, payload_dict = await openai_extractor.extract_payload(input_data)
    
    assert schema_name == "HttpCall"
    assert payload_dict["url"] == "https://api.example.com/users"
    assert payload_dict["method"] == "POST"
    assert payload_dict["headers"] == {
        "Content-Type": "application/json",
        "Authorization": "Bearer token123"
    }
    assert payload_dict["body"] == {
        "name": "Alice",
        "email": "alice@example.com",
        "age": 28
    }
    assert isinstance(payload_dict, dict)

@pytest.mark.asyncio
async def test_extract_recurring_schedule_with_timezone(openai_extractor: OpenAIExtractor):
    """
    Test the extract_schedule method for a recurring schedule with timezone handling using real OpenAI API.
    """
    input_data = "I'm in Shanghai, and I need to send a weekly report every Friday at 3 PM"
    
    schedule = await openai_extractor.extract_schedule(input_data)
    
    assert isinstance(schedule, RecurringSchedule)
    assert schedule.cron_expression in ["0 7 * * 5", "0 7 * * FRI"]  # 3 PM Shanghai time is 7 AM UTC
