import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Boolean,
    ForeignKey,
    Text,
    BigInteger,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан в переменных окружения")

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_recycle=300,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class House(Base):
    __tablename__ = "houses"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    join_code = Column(String, unique=True, index=True, nullable=False)

    users = relationship("User", back_populates="house")
    templates = relationship("TaskTemplate", back_populates="house")
    onboarding_text = relationship(
        "HouseOnboarding",
        uselist=False,
        back_populates="house",
        cascade="all, delete-orphan",
    )


class HouseOnboarding(Base):
    __tablename__ = "house_onboarding"

    id = Column(Integer, primary_key=True)
    house_id = Column(Integer, ForeignKey("houses.id"), nullable=False, unique=True)
    text = Column(Text, nullable=False)

    house = relationship("House", back_populates="onboarding_text")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)

    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    username = Column(String, nullable=True)
    full_name = Column(String, nullable=True)

    house_id = Column(Integer, ForeignKey("houses.id"), nullable=True)
    is_house_owner = Column(Boolean, default=False)

    house = relationship("House", back_populates="users")


class TaskTemplate(Base):
    __tablename__ = "task_templates"

    id = Column(Integer, primary_key=True)
    house_id = Column(Integer, ForeignKey("houses.id"), nullable=False)

    title = Column(String, nullable=False)
    description = Column(String, nullable=True)
    periodicity = Column(String, nullable=False, default="daily")
    points = Column(Integer, nullable=False, default=1)
    deleted = Column(Boolean, default=False)
    start_date = Column(Date, nullable=True)

    house = relationship("House", back_populates="templates")
    instances = relationship("TaskInstance", back_populates="template")


class TaskInstance(Base):
    __tablename__ = "task_instances"

    id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey("task_templates.id"), nullable=False)
    date = Column(Date, nullable=False, index=True)
    status = Column(String, nullable=False, default="free")   # free/in_progress/done
    priority = Column(String, nullable=False, default="normal")

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
    print("Init DB, URL:", DATABASE_URL)
    try:
        with engine.connect() as conn:
            print("✅ БД подключена:", conn.engine.url)
        print("Создаю таблицы...")
        Base.metadata.create_all(bind=engine)
        print("✅ Таблицы созданы")
    except Exception as e:
        print("❌ Ошибка БД:", repr(e))


LOCAL_TZ = ZoneInfo("Europe/Moscow")

def get_today() -> date:
    return datetime.now(LOCAL_TZ).date()
