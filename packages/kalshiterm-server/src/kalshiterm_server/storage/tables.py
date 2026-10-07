"""SQLAlchemy Core definitions mirroring the migrations (a test checks they cannot drift)."""

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    Text,
    func,
)

metadata = MetaData()

series = Table(
    "series",
    metadata,
    Column("ticker", Text, primary_key=True),
    Column("title", Text, nullable=False),
    Column("frequency", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("tags", ARRAY(Text)),
    Column("fee_type", Text),
    Column("fee_multiplier_e6", BigInteger),
    Column("source_updated_at", DateTime(timezone=True)),
    Column("first_seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

events = Table(
    "events",
    metadata,
    Column("event_ticker", Text, primary_key=True),
    Column("series_ticker", Text, nullable=False),
    Column("title", Text, nullable=False),
    Column("sub_title", Text, nullable=False, server_default=""),
    Column("mutually_exclusive", Boolean, nullable=False),
    Column("strike_date", DateTime(timezone=True)),
    Column("strike_period", Text),
    Column("source_updated_at", DateTime(timezone=True)),
    Column("first_seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

markets = Table(
    "markets",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", Text, nullable=False, unique=True),
    Column("event_ticker", Text, nullable=False),
    Column("market_type", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("yes_sub_title", Text, nullable=False, server_default=""),
    Column("no_sub_title", Text, nullable=False, server_default=""),
    Column("created_time", DateTime(timezone=True)),
    Column("updated_time", DateTime(timezone=True)),
    Column("open_time", DateTime(timezone=True)),
    Column("close_time", DateTime(timezone=True)),
    Column("latest_expiration_time", DateTime(timezone=True)),
    Column("result", Text, nullable=False, server_default=""),
    Column("settlement_value_e6", BigInteger),
    Column("settlement_ts", DateTime(timezone=True)),
    Column("rules_primary", Text, nullable=False, server_default=""),
    Column("rules_secondary", Text, nullable=False, server_default=""),
    Column("strike_type", Text),
    Column("floor_strike_e6", BigInteger),
    Column("cap_strike_e6", BigInteger),
    Column("exchange_index", Integer),
    Column("first_seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

discovery_state = Table(
    "discovery_state",
    metadata,
    Column("key", Text, primary_key=True),
    Column("value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
