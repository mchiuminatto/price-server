import enum
from sqlalchemy import Column, Integer, Enum, DateTime, Numeric, ForeignKey, Index
from sqlalchemy.orm import relationship
from app.database import Base


class TimeFrame(str, enum.Enum):
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN1 = "MN1"
    CUSTOM = "CUSTOM"


class PriceType(str, enum.Enum):
    TICK = "TICK"
    OHLC = "OHLC"
    OHLC_NON_REGULAR = "OHLC_NON_REGULAR"


class TickData(Base):
    __tablename__ = "tick_data"
    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    bid = Column(Numeric(20, 8), nullable=False)
    ask = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(20, 8), nullable=True)
    instrument = relationship("Instrument", back_populates="tick_data")
    __table_args__ = (Index("ix_tick_instrument_timestamp", "instrument_id", "timestamp"),)


class OHLCData(Base):
    __tablename__ = "ohlc_data"
    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    open = Column(Numeric(20, 8), nullable=False)
    high = Column(Numeric(20, 8), nullable=False)
    low = Column(Numeric(20, 8), nullable=False)
    close = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(20, 8), nullable=True)
    timeframe = Column(Enum(TimeFrame), nullable=False)
    price_type = Column(Enum(PriceType), nullable=False, default=PriceType.OHLC)
    instrument = relationship("Instrument", back_populates="ohlc_data")
    __table_args__ = (
        Index("ix_ohlc_instrument_timestamp_tf", "instrument_id", "timestamp", "timeframe"),
    )
