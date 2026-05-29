"""Runtime state for the standalone MQTT adapter."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class PresenceRecord:
    """Presence state for a partner bot."""

    bot_id: str
    bot_name: str = ""
    status: str = "offline"
    last_seen: float = field(default_factory=time.time)
    is_known_partner: bool = False


@dataclass(slots=True)
class RelaySession:
    """Minimal relay session state."""

    conversation_id: str
    peer_bot_id: str
    channel: str
    intent: str
    state: str | None = None
    terminal: bool = False
    expect_reply: bool = False
    reply_budget: int = 0
    allowed_responders: list[str] = field(default_factory=list)
    phase: str | None = None
    turn_count: int = 0
    max_turns: int = 6
    cooldown_seconds: int = 0
    cooldown_until: float = 0.0
    updated_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class RelayTransactionRecord:
    """Transaction record for relay protocol state."""

    conversation_id: str
    trace_id: str
    from_bot: str
    to_bot: str
    current_state: str
    final_intent: str | None = None
    topic: str = ""
    summary: str = ""


DEDUP_CACHE: dict[str, float] = {}
PRESENCE_TABLE: dict[str, PresenceRecord] = {}
SESSION_TABLE: dict[str, RelaySession] = {}
AUDIT_LOG: list[dict[str, object]] = []
TRANSACTION_LOG: dict[str, RelayTransactionRecord] = {}


def reset_state() -> None:
    """Clear module-level state for plugin-local tests."""

    DEDUP_CACHE.clear()
    PRESENCE_TABLE.clear()
    SESSION_TABLE.clear()
    AUDIT_LOG.clear()
    TRANSACTION_LOG.clear()


def remember_message(message_id: str, ttl_seconds: int = 3600) -> bool:
    """Record a message id if it has not been seen recently."""

    now = time.time()
    expired = [key for key, seen_at in DEDUP_CACHE.items() if now - seen_at > ttl_seconds]
    for key in expired:
        DEDUP_CACHE.pop(key, None)
    if message_id in DEDUP_CACHE:
        return False
    DEDUP_CACHE[message_id] = now
    return True


def upsert_presence(record: PresenceRecord) -> None:
    """Store presence state."""

    PRESENCE_TABLE[record.bot_id] = record


def save_session(session: RelaySession) -> None:
    """Store relay session state."""

    session.updated_at = time.time()
    SESSION_TABLE[session.conversation_id] = session


def get_session(conversation_id: str) -> RelaySession | None:
    """Return relay session state by id."""

    return SESSION_TABLE.get(conversation_id)


def audit(event: str, **data: object) -> None:
    """Append a lightweight audit entry."""

    AUDIT_LOG.append({"event": event, "time": time.time(), **data})


def save_transaction_record(record: RelayTransactionRecord) -> None:
    """Persist transaction log entry."""

    TRANSACTION_LOG[record.conversation_id] = record
