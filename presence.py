"""Presence tracking and allowlist checks for MQTT relay."""

from __future__ import annotations

import time

from . import store
from .config import MqttAdapterConfig
from .envelope import RelayEnvelope


class PresenceManager:
    """Manage partner presence using module-level state."""

    def __init__(self, config: MqttAdapterConfig) -> None:
        """Initialize presence manager with typed config."""

        self.config = config

    def is_allowed(self, bot_id: str) -> bool:
        """Return whether a bot id is allowed."""

        if not self.config.presence.require_known_partner:
            return True
        return bot_id in self.config.presence.allowed_partner_bots

    def update_from_envelope(self, envelope: RelayEnvelope) -> None:
        """Update presence from a system envelope."""

        status = str(envelope.payload.get("status") or "online")
        store.upsert_presence(
            store.PresenceRecord(
                bot_id=envelope.from_bot,
                bot_name=envelope.from_bot_name,
                status=status,
                last_seen=time.time(),
                is_known_partner=self.is_allowed(envelope.from_bot),
            )
        )

    def build_presence_envelope(self, *, status: str) -> RelayEnvelope:
        """Build a local presence envelope."""

        return RelayEnvelope(
            from_bot=self.config.mqtt.bot_id,
            from_bot_name=self.config.mqtt.bot_name,
            to_bot="*",
            to_bot_name="*",
            channel="system",
            intent="presence_update",
            terminal=True,
            expect_reply=False,
            payload={"status": status},
        )
