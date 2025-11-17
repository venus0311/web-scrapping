import uuid
from sqlalchemy import create_engine, Column, String, Text, Boolean, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from datetime import datetime
import os
from sqlalchemy import JSON


# ---------- Database Setup ----------
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./database.db")

if not DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_pre_ping=True
    )
else:
    engine = create_engine(
        DATABASE_URL,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_pre_ping=True,
        pool_recycle=1800
    )


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ---------- Models ----------

# class ProcessEntry(Base):
#     __tablename__ = "process_entries"

#     id = Column(String, primary_key=True, index=True)
#     name = Column(String)
#     url = Column(String)
#     status = Column(String)
#     is_stopped = Column(Boolean, default=False) 
#     last_processed_row = Column(Integer, default=0, nullable=False)  
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
#     error_message = Column(Text, nullable=True) 
#     input_data = Column(JSON, nullable=True)


# class ProcessItem(Base):
#     __tablename__ = "process_items"
#     id = Column(Integer, primary_key=True, index=True)
#     entry_id = Column(String, ForeignKey("process_entries.id"))
#     value = Column(String)  
#     status = Column(String, default="unprocessed")  
#     result = Column(JSON) 


class ProcessEntry(Base):
    __tablename__ = "process_entries"
    
    id = Column(String, primary_key=True, index=True)
    name = Column(String)
    url = Column(String)
    status = Column(String)
    is_stopped = Column(Boolean, default=False) 
    last_processed_row = Column(Integer, default=0, nullable=False)  
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    error_message = Column(Text, nullable=True) 
    input_data = Column(JSON, nullable=True)

    items = relationship(
        "ProcessItem",
        back_populates="entry",
        cascade="all, delete-orphan"
    )


class ProcessItem(Base):
    __tablename__ = "process_items"
    id = Column(Integer, primary_key=True, index=True)
    entry_id = Column(String, ForeignKey("process_entries.id"))
    value = Column(String)  
    status = Column(String, default="unprocessed")  
    result = Column(JSON)    

    entry = relationship("ProcessEntry", back_populates="items")


def init_db():
    Base.metadata.create_all(bind=engine)
