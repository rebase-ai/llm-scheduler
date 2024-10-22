import json
from typing import List, Optional
from sqlalchemy import create_engine, Column, String, DateTime, Boolean, JSON, ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.future import select
from llm_scheduler.domain.task import Task, OneTimeSchedule, RecurringSchedule, ImmediateSchedule, ScheduleType
from llm_scheduler.domain.job import Job, JobStatus
from llm_scheduler.storages.protocol import Storage

Base = declarative_base()

class TaskModel(Base):
    __tablename__ = 'tasks'

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String)
    created_at = Column(DateTime(timezone=True), nullable=False)
    schedule_type = Column(String, nullable=False)
    schedule = Column(JSON, nullable=False)
    payload = Column(JSON, nullable=False)
    payload_schema_name = Column(String, nullable=False)
    meta = Column(JSON)
    is_active = Column(Boolean, default=True)

    jobs = relationship("JobModel", back_populates="task")

class JobModel(Base):
    __tablename__ = 'jobs'

    id = Column(String, primary_key=True)
    task_id = Column(String, ForeignKey('tasks.id'), nullable=False)
    status = Column(String, nullable=False)
    result = Column(JSON)
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))

    task = relationship("TaskModel", back_populates="jobs", lazy="selectin")

class SqlAlchemyStorage(Storage):
    def __init__(self, db_url: str):
        self.engine = create_async_engine(db_url)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def create_task(self, task: Task) -> str:
        async with self.async_session() as session:
            db_task = TaskModel(
                id=task.id,
                name=task.name,
                description=task.description,
                created_at=task.created_at,
                schedule_type=task.schedule_type.value,
                schedule=task.schedule.model_dump_json(),
                payload=task.payload,
                payload_schema_name=task.payload_schema_name,
                meta=task.meta,
                is_active=task.is_active
            )
            session.add(db_task)
            await session.commit()
            return task.id

    async def get_task(self, task_id: str) -> Optional[Task]:
        async with self.async_session() as session:
            result = await session.execute(select(TaskModel).filter_by(id=task_id))
            db_task = result.scalar_one_or_none()
            if db_task:
                return self._db_to_task(db_task)
            return None

    async def update_task(self, task: Task) -> bool:
        async with self.async_session() as session:
            result = await session.execute(select(TaskModel).filter_by(id=task.id))
            db_task = result.scalar_one_or_none()
            if db_task:
                db_task.name = task.name
                db_task.description = task.description
                db_task.schedule_type = task.schedule_type.value
                db_task.schedule = task.schedule.model_dump_json()
                db_task.payload = task.payload
                db_task.payload_schema_name = task.payload_schema_name
                db_task.meta = task.meta
                db_task.is_active = task.is_active
                await session.commit()
                return True
            return False

    async def delete_task(self, task_id: str) -> bool:
        async with self.async_session() as session:
            result = await session.execute(select(TaskModel).filter_by(id=task_id))
            db_task = result.scalar_one_or_none()
            if db_task:
                await session.delete(db_task)
                await session.commit()
                return True
            return False

    async def list_tasks(self, limit: int = 20, offset: int = 0) -> List[Task]:
        async with self.async_session() as session:
            result = await session.execute(select(TaskModel).offset(offset).limit(limit).order_by(TaskModel.created_at.desc()))
            return [self._db_to_task(db_task) for db_task in result.scalars()]

    async def create_job(self, job: Job) -> str:
        async with self.async_session() as session:
            db_job = JobModel(
                id=job.id,
                task_id=job.task.id,
                status=job.status.value,
                result=job.result,
                start_time=job.start_time,
                end_time=job.end_time
            )
            session.add(db_job)
            await session.commit()
            return job.id

    async def get_job(self, job_id: str) -> Optional[Job]:
        async with self.async_session() as session:
            result = await session.execute(select(JobModel).filter_by(id=job_id))
            db_job = result.scalar_one_or_none()
            if db_job:
                return self._db_to_job(db_job)
            return None

    async def update_job(self, job: Job) -> bool:
        async with self.async_session() as session:
            result = await session.execute(select(JobModel).filter_by(id=job.id))
            db_job = result.scalar_one_or_none()
            if db_job:
                db_job.status = job.status.value
                db_job.result = job.result
                db_job.start_time = job.start_time
                db_job.end_time = job.end_time
                await session.commit()
                return True
            return False

    async def list_recent_jobs(self, task_id: str, limit: int = 20) -> List[Job]:
        async with self.async_session() as session:
            result = await session.execute(
                select(JobModel)
                .filter_by(task_id=task_id)
                .order_by(JobModel.start_time.desc())
                .limit(limit)
            )
            return [self._db_to_job(db_job) for db_job in result.scalars()]

    async def get_recent_job(self, task_id: str) -> Optional[Job]:
        async with self.async_session() as session:
            result = await session.execute(
                select(JobModel)
                .filter_by(task_id=task_id)
                .order_by(JobModel.start_time.desc())
                .limit(1)
            )
            db_job = result.scalar_one_or_none()
            if db_job:
                return self._db_to_job(db_job)
            return None

    def _db_to_task(self, db_task: TaskModel) -> Task:
        schedule_type = ScheduleType(db_task.schedule_type)
        if schedule_type == ScheduleType.ONE_TIME:
            schedule = OneTimeSchedule(**json.loads(db_task.schedule))
        elif schedule_type == ScheduleType.RECURRING:
            schedule = RecurringSchedule(**json.loads(db_task.schedule))
        elif schedule_type == ScheduleType.IMMEDIATE:
            schedule = ImmediateSchedule()
        else:
            raise ValueError(f"Unsupported schedule type: {schedule_type}")

        return Task(
            id=db_task.id,
            name=db_task.name,
            description=db_task.description,
            created_at=db_task.created_at,
            schedule=schedule,
            payload=db_task.payload,
            payload_schema_name=db_task.payload_schema_name,
            meta=db_task.meta,
            is_active=db_task.is_active
        )

    def _db_to_job(self, db_job: JobModel) -> Job:
        return Job(
            id=db_job.id,
            task=self._db_to_task(db_job.task),
            status=JobStatus(db_job.status),
            result=db_job.result,
            start_time=db_job.start_time,
            end_time=db_job.end_time
        )


class InMemoryStorage(SqlAlchemyStorage):
    def __init__(self):
        super().__init__("sqlite+aiosqlite:///:memory:")

