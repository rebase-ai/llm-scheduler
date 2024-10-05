from typing import Any, Protocol

from llm_scheduler.domain.job import Job


class JobExecutor(Protocol):
    """
    Protocol class for job executors.
    """

    def execute(self, job: Job) -> None:
        """
        Execute the given job.

        Args:
            job (Job): The job to be executed.
        """
        ...

    async def async_execute(self, job: Job) -> None:
        """
        Asynchronously execute the given job.

        Args:
            job (Job): The job to be executed.
        """
        ...

    @staticmethod
    def supported_schema() -> str:
        """
        Return the name of the schema this executor supports.
        """
        ...