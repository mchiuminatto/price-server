import enum
from sqlalchemy import Column, Integer, String, Enum, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class AssetType(str, enum.Enum):
    CURRENCY = "CURRENCY"
    CRYPTO = "CRYPTO"
    COMMODITY = "COMMODITY"
    EQUITY = "EQUITY"


class Instrument(Base):
    __tablename__ = "instruments"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    asset_type = Column(Enum(AssetType), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    tick_data = relationship("TickData", back_populates="instrument")
    ohlc_data = relationship("OHLCData", back_populates="instrument")
