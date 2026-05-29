"""Policy rules for MQTT relay control fields."""

from __future__ import annotations

from .envelope import RelayEnvelope


class PolicyEngine:
    """Apply deterministic control-field rules."""

    def apply_outbound(self, envelope: RelayEnvelope) -> RelayEnvelope:
        """Apply terminal and reply-budget policy to an outbound envelope."""

        if envelope.channel == "transaction" and envelope.intent == "notify":
            envelope.terminal = True
            envelope.expect_reply = False
            envelope.reply_budget = 0
            envelope.allowed_responders = []
            envelope.state = envelope.state or "closed"
        elif envelope.channel == "transaction" and envelope.intent == "request":
            envelope.terminal = False
            envelope.expect_reply = True
            envelope.state = envelope.state or "pending_reply"
            if envelope.reply_budget <= 0:
                envelope.reply_budget = 3
        if envelope.terminal:
            envelope.expect_reply = False
        if not envelope.allowed_responders and envelope.intent != "notify":
            envelope.expect_reply = False
        return envelope

    def should_auto_reply(self, relay_context: dict[str, object] | None) -> bool:
        """Return whether a relay message may trigger bot auto-reply."""

        if not relay_context:
            return False
        if relay_context.get("terminal") is True:
            return False
        return bool(relay_context.get("expect_reply", False))
