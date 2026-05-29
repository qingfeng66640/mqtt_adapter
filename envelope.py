"""Relay envelope model and validation helpers for the MQTT adapter."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import uuid4

RelayChannel = Literal["system", "transaction", "social"]


@dataclass(slots=True)
class RelayEnvelope:
    """Protocol envelope exchanged between bots over MQTT."""

    protocol_version: str = "1.0"
    message_id: str = field(default_factory=lambda: uuid4().hex)
    conversation_id: str = field(default_factory=lambda: uuid4().hex)
    trace_id: str = field(default_factory=lambda: uuid4().hex)
    parent_message_id: str | None = None
    from_bot: str = ""
    from_bot_name: str = ""
    to_bot: str = ""
    to_bot_name: str = ""
    sender_instance_id: str = ""
    target_scope: str = "direct"
    channel: RelayChannel = "transaction"
    intent: str = "notify"
    expect_reply: bool = False
    reply_budget: int = 0
    hop: int = 0
    ttl: int = 4
    no_relay: bool = False
    terminal: bool = True
    allowed_responders: list[str] = field(default_factory=list)
    cooldown_seconds: int = 0
    reply_contract: dict[str, Any] = field(default_factory=dict)
    state: str | None = None
    phase: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RelayEnvelope":
        """Build an envelope from a dictionary."""

        known = {field_name for field_name in cls.__dataclass_fields__}
        return cls(**{key: value for key, value in data.items() if key in known})

    def to_dict(self) -> dict[str, Any]:
        """Serialize the envelope to a JSON-friendly dictionary."""

        return {
            "protocol_version": self.protocol_version,
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "trace_id": self.trace_id,
            "parent_message_id": self.parent_message_id,
            "from_bot": self.from_bot,
            "from_bot_name": self.from_bot_name,
            "to_bot": self.to_bot,
            "to_bot_name": self.to_bot_name,
            "sender_instance_id": self.sender_instance_id,
            "target_scope": self.target_scope,
            "channel": self.channel,
            "intent": self.intent,
            "expect_reply": self.expect_reply,
            "reply_budget": self.reply_budget,
            "hop": self.hop,
            "ttl": self.ttl,
            "no_relay": self.no_relay,
            "terminal": self.terminal,
            "allowed_responders": list(self.allowed_responders),
            "cooldown_seconds": self.cooldown_seconds,
            "reply_contract": dict(self.reply_contract),
            "state": self.state,
            "phase": self.phase,
            "payload": dict(self.payload),
            "created_at": self.created_at,
        }

    @property
    def text(self) -> str:
        """Return text payload content."""

        value = self.payload.get("text", "")
        return value if isinstance(value, str) else str(value)

    def validate(self) -> None:
        """Validate required envelope fields."""

        if not self.message_id:
            raise ValueError("message_id is required")
        if not self.from_bot:
            raise ValueError("from_bot is required")
        if not self.to_bot:
            raise ValueError("to_bot is required")
        if self.hop > self.ttl:
            raise ValueError("hop exceeds ttl")
        if self.reply_budget < 0:
            raise ValueError("reply_budget must not be negative")

    def increment_hop(self) -> "RelayEnvelope":
        """Return a copy-like envelope with hop incremented."""

        data = self.to_dict()
        data["hop"] = self.hop + 1
        return RelayEnvelope.from_dict(data)
