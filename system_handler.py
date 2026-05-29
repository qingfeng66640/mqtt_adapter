"""System-channel short-path handling for MQTT relay."""

from __future__ import annotations

from . import store
from .envelope import RelayEnvelope
from .presence import PresenceManager


class SystemChannelHandler:
    """Handle relay system envelopes without entering LLM flow."""

    def __init__(self, presence_manager: PresenceManager) -> None:
        """Initialize handler with a presence manager."""

        self.presence_manager = presence_manager

    def handle(self, envelope: RelayEnvelope) -> bool:
        """Handle a system envelope and return whether it was consumed."""

        if envelope.channel != "system":
            return False
        if envelope.intent == "presence_update":
            self.presence_manager.update_from_envelope(envelope)
        elif envelope.intent in {"cancel", "close", "error", "ack", "heartbeat", "typing"}:
            store.audit("system_event", intent=envelope.intent, from_bot=envelope.from_bot)
        return True
