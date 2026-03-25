from datetime import date, datetime
import os

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Boolean,
    ForeignKey,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Берём адрес БД только из переменной окружения DATABASE_URL.
# Пример значения (Railway):
# postgresql://postgres:...@crossover.proxy.rlwy.net:11538/railway
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Please configure it in environment variables.")

# Для Postgres никакие специальные connect_args не нужны
engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, index=True, nullable=False)
    username = Column(String, nullable=True)
    full_name = Column(String, nullable=True)


class TaskTemplate(Base):
    __tablename__ = "task_templates"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=True)
    periodicity = Column(String, nullable=False, default="daily")
    points = Column(Integer, nullable=False, default=1)
    active = Column(Boolean, default=True)

    instances = relationship("TaskInstance", back_populates="template")


class TaskInstance(Base):
    __tablename__ = "task_instances"

    id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey("task_templates.id"), nullable=False)
    date = Column(Date, nullable=False, index=True)
    status = Column(String, nullable=False, default="free")  # free, in_progress, done
    priority = Column(String, nullable=False, default="normal")  # normal, high

    assigned_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    done_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    done_at = Column(DateTime, nullable=True)

    template = relationship("TaskTemplate", back_populates="instances")
    assigned_user = relationship("User", foreign_keys=[assigned_user_id])
    done_by_user = relationship("User", foreign_keys=[done_by_user_id])


class Completion(Base):
    __tablename__ = "completions"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    task_instance_id = Column(Integer, ForeignKey("task_instances.id"), nullable=False)
    points = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User")
    task_instance = relationship("TaskInstance")


def init_db():
    Base.metadata.create_all(bind=engine)


def get_today() -> date:
    return date.today()
