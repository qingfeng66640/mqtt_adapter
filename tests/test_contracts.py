"""Contract tests for the standalone MQTT adapter plugin."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from mofox_wire import MessageEnvelope

from plugins.mqtt_adapter import store
from plugins.mqtt_adapter.config import MqttAdapterConfig, PartnerSection
from plugins.mqtt_adapter.envelope import RelayEnvelope
from plugins.mqtt_adapter.plugin import MqttAdapter, MqttAdapterPlugin
from plugins.mqtt_adapter.policy import PolicyEngine
from plugins.mqtt_adapter.presence import PresenceManager
from plugins.mqtt_adapter.session import SessionManager
from plugins.mqtt_adapter.system_handler import SystemChannelHandler


class DummySink:
    """Minimal CoreSink stub for adapter tests."""

    captured: list[MessageEnvelope]

    def __init__(self) -> None:
        """Initialize captured envelope list."""

        self.captured = []

    async def send(self, envelope: MessageEnvelope) -> None:
        """Capture an envelope sent by the adapter."""

        self.captured.append(envelope)


class DummyMqttMessage:
    """MQTT message test double."""

    def __init__(self, topic: str, payload: str) -> None:
        """Initialize topic and encoded payload."""

        self.topic = topic
        self.payload = payload.encode("utf-8")


class StubClient:
    """MQTT client test double."""

    def __init__(self, connected: bool = True) -> None:
        """Initialize connection and stop flags."""

        self.connected = connected
        self.loop_stopped = False
        self.disconnected = False

    def is_connected(self) -> bool:
        """Return configured connection state."""

        return self.connected

    def loop_stop(self) -> None:
        """Record loop stop."""

        self.loop_stopped = True

    def disconnect(self) -> None:
        """Record disconnect."""

        self.disconnected = True


def build_config() -> MqttAdapterConfig:
    """Build a valid MQTT adapter test config."""

    config = MqttAdapterConfig()
    config.mqtt.bot_id = "223123"
    config.mqtt.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    return config


def build_adapter() -> MqttAdapter:
    """Build a configured MQTT adapter test instance."""

    plugin = MqttAdapterPlugin(build_config())
    return MqttAdapter(core_sink=DummySink(), plugin=plugin)


def test_manifest_and_plugin_identity() -> None:
    """Manifest and plugin identities must match loader expectations."""

    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("manifest.json").read_text(encoding="utf-8"))
    assert manifest["name"] == "mqtt_adapter"
    assert manifest["python_dependencies"] == ["paho-mqtt>=2.0"]
    assert manifest["include"] == [
        {
            "component_type": "adapter",
            "component_name": "mqtt_adapter",
            "dependencies": [],
            "enabled": True,
        }
    ]
    assert MqttAdapterPlugin.plugin_name == "mqtt_adapter"
    assert MqttAdapter.adapter_name == "mqtt_adapter"
    assert MqttAdapter in MqttAdapterPlugin(build_config()).get_components()


def test_config_partner_lookup_uses_bot_id() -> None:
    """Partner lookup should route by bot id only."""

    config = build_config()
    partner = config.partner_by_id("114514")
    assert partner is not None
    assert partner.bot_name == "流光"
    assert config.first_allowed_partner() is partner


def test_config_default_path_matches_framework_convention() -> None:
    """Framework reads config/plugins/{plugin_name}/config.toml by convention."""

    MqttAdapterConfig._plugin_ = "mqtt_adapter"
    try:
        path = MqttAdapterConfig.get_default_path()
        assert path is not None
        assert path.parts[-3:] == ("plugins", "mqtt_adapter", "config.toml")
    finally:
        if hasattr(MqttAdapterConfig, "_plugin_"):
            delattr(MqttAdapterConfig, "_plugin_")


def test_relay_envelope_roundtrip_and_validation() -> None:
    """RelayEnvelope should roundtrip and validate required fields."""

    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", payload={"text": "hi"})
    rebuilt = RelayEnvelope.from_dict(envelope.to_dict())
    rebuilt.validate()
    assert rebuilt.from_bot == "223123"
    assert rebuilt.text == "hi"


def test_relay_envelope_increment_hop() -> None:
    """Hop increment should return a new envelope."""

    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", hop=0, ttl=4)
    incremented = envelope.increment_hop()
    assert incremented.hop == 1
    assert envelope.hop == 0


def test_relay_envelope_hop_exceeds_ttl_validation() -> None:
    """Validation should reject envelopes exceeding TTL."""

    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", hop=5, ttl=4)
    try:
        envelope.validate()
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "hop exceeds ttl" in str(exc)


def test_store_dedup_and_reset() -> None:
    """Store should deduplicate message ids and reset state."""

    store.reset_state()
    assert store.remember_message("m1") is True
    assert store.remember_message("m1") is False
    store.reset_state()
    assert store.DEDUP_CACHE == {}


def test_policy_notify_is_one_way() -> None:
    """Notify envelopes should be terminal one-way messages."""

    envelope = RelayEnvelope(
        from_bot="223123",
        to_bot="114514",
        intent="notify",
        channel="transaction",
        expect_reply=True,
        reply_budget=9,
        terminal=False,
    )
    result = PolicyEngine().apply_outbound(envelope)
    assert result.terminal is True
    assert result.expect_reply is False
    assert result.reply_budget == 0


def test_session_manager_builds_request() -> None:
    """SessionManager should build outbound request envelopes."""

    store.reset_state()
    envelope = SessionManager().build_outbound_envelope(
        message_envelope={
            "message_info": {
                "platform": "mqtt",
                "extra": {"relay_context": {"intent": "request", "channel": "transaction"}},
            },
            "message_segment": [{"type": "text", "data": "请帮我处理一下"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )
    assert envelope.intent == "request"
    assert envelope.expect_reply is True
    assert envelope.allowed_responders == ["114514"]


def test_presence_and_system_handler_short_path() -> None:
    """Presence system events should be consumed before core dispatch."""

    store.reset_state()
    config = build_config()
    presence = PresenceManager(config)
    handler = SystemChannelHandler(presence)
    consumed = handler.handle(
        RelayEnvelope(
            from_bot="114514",
            from_bot_name="流光",
            to_bot="*",
            to_bot_name="*",
            channel="system",
            intent="presence_update",
            payload={"status": "online"},
        )
    )
    assert consumed is True
    assert store.PRESENCE_TABLE["114514"].status == "online"


def test_mqtt_presence_inbound_respects_system_log_config(monkeypatch: Any) -> None:
    """Presence inbound logging should respect config switch."""

    adapter = build_adapter()
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr("plugins.mqtt_adapter.plugin.logger.info", lambda message: calls.append(("info", message)))
    adapter._event_loop = SimpleNamespace(is_closed=lambda: False)

    def consume_coro(coro: Any, *_args: Any, **_kwargs: Any) -> None:
        coro.close()

    monkeypatch.setattr("plugins.mqtt_adapter.plugin.asyncio.run_coroutine_threadsafe", consume_coro)
    adapter._on_mqtt_message_callback(None, None, DummyMqttMessage("bot/presence/114514", "{}"))
    adapter.mqtt_config.mqtt.show_system_message_logs = False
    adapter._on_mqtt_message_callback(None, None, DummyMqttMessage("bot/presence/114514", "{}"))
    adapter._on_mqtt_message_callback(None, None, DummyMqttMessage("bot/223123/inbox", "{}"))
    assert len(calls) == 2
    assert "bot/presence/114514" in calls[0][1]
    assert "bot/223123/inbox" in calls[1][1]


def test_adapter_rejects_wrong_target_or_unknown_partner() -> None:
    """Adapter should drop wrong-target and unknown-partner messages."""

    adapter = build_adapter()
    wrong_target = asyncio.run(
        adapter.from_platform_message(
            {
                "from_bot": "223123",
                "from_bot_name": "清风",
                "to_bot": "999999",
                "to_bot_name": "别的 bot",
                "channel": "transaction",
                "intent": "notify",
                "message_id": "m-wrong-target",
                "conversation_id": "c1",
                "trace_id": "t1",
                "payload": {"text": "hello"},
            }
        )
    )
    assert wrong_target is None

    unknown_partner = asyncio.run(
        adapter.from_platform_message(
            {
                "from_bot": "777777",
                "from_bot_name": "陌生 bot",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "notify",
                "message_id": "m-unknown-partner",
                "conversation_id": "c2",
                "trace_id": "t2",
                "payload": {"text": "hello"},
            }
        )
    )
    assert unknown_partner is None


def test_adapter_unknown_partner_gets_system_error_without_state_changes() -> None:
    """Unknown partners should receive system error without creating sessions."""

    store.reset_state()
    adapter = build_adapter()
    published: list[RelayEnvelope] = []

    async def publish(envelope: RelayEnvelope) -> None:
        published.append(envelope)

    adapter.publish_relay_envelope = publish  # type: ignore[method-assign]
    result = asyncio.run(
        adapter.from_platform_message(
            {
                "from_bot": "777777",
                "from_bot_name": "陌生 bot",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "request",
                "message_id": "m-unknown-error",
                "conversation_id": "c-unknown-error",
                "trace_id": "t-unknown-error",
                "payload": {"text": "hello"},
            }
        )
    )
    assert result is None
    assert len(published) == 1
    error = published[0]
    assert error.channel == "system"
    assert error.intent == "error"
    assert error.from_bot == "223123"
    assert error.to_bot == "777777"
    assert error.conversation_id == "c-unknown-error"
    assert error.trace_id == "t-unknown-error"
    assert error.parent_message_id == "m-unknown-error"
    assert error.terminal is True
    assert error.expect_reply is False
    assert error.reply_budget == 0
    assert error.allowed_responders == []
    assert error.no_relay is True
    assert error.payload["code"] == "sender_not_allowed"
    assert store.SESSION_TABLE == {}
    assert store.TRANSACTION_LOG == {}
    assert store.AUDIT_LOG[-1]["event"] == "sender_not_allowed"


def test_adapter_accepts_allowed_partner_and_returns_message_envelope() -> None:
    """Allowed partner messages should become incoming MessageEnvelope."""

    store.reset_state()
    adapter = build_adapter()
    envelope = asyncio.run(
        adapter.from_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
                "allowed_responders": ["223123"],
                "hop": 0,
                "ttl": 4,
                "message_id": "m-ok",
                "conversation_id": "c-ok",
                "trace_id": "t-ok",
                "payload": {"text": "请帮我处理一下"},
            }
        )
    )
    assert envelope is not None
    message_info = envelope.get("message_info") or {}
    user_info = message_info.get("user_info") if isinstance(message_info, dict) else {}
    assert user_info.get("user_id") == "114514"
    extra = message_info.get("extra") if isinstance(message_info, dict) else {}
    relay_context = extra.get("relay_context") if isinstance(extra, dict) else {}
    assert relay_context.get("allowed_responders") == ["223123"]
    assert relay_context.get("peer_bot_id") == "114514"
    session = store.SESSION_TABLE["c-ok"]
    assert session.peer_bot_id == "114514"
    assert session.state == "pending_reply"
    assert session.allowed_responders == ["223123"]


def test_adapter_drops_orphan_transaction_continuation_before_core() -> None:
    """Orphan transaction continuations should not enter core."""

    store.reset_state()
    adapter = build_adapter()
    envelope = asyncio.run(
        adapter.from_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "accept",
                "expect_reply": False,
                "reply_budget": 99,
                "terminal": True,
                "allowed_responders": ["223123"],
                "state": "accepted",
                "hop": 0,
                "ttl": 4,
                "message_id": "m-orphan-accept",
                "conversation_id": "c-orphan-accept",
                "trace_id": "t-orphan-accept",
                "payload": {"text": "我接受你的邀请。"},
            }
        )
    )
    assert envelope is None
    assert store.SESSION_TABLE == {}
    assert store.TRANSACTION_LOG == {}
    assert store.AUDIT_LOG[-1]["event"] == "orphan_transaction_continuation"


def test_adapter_increments_hop_on_inbound() -> None:
    """Inbound processing should increment hop for TTL protection."""

    adapter = build_adapter()
    envelope = asyncio.run(
        adapter.from_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "notify",
                "hop": 0,
                "ttl": 4,
                "message_id": "m-hop",
                "conversation_id": "c-hop",
                "trace_id": "t-hop",
                "payload": {"text": "hop test"},
            }
        )
    )
    assert envelope is not None
    extra = (envelope.get("message_info") or {}).get("extra", {})
    relay_envelope = extra.get("relay_envelope", {}) if isinstance(extra, dict) else {}
    assert relay_envelope.get("hop") == 1


def test_adapter_uses_standard_on_platform_message_pipeline() -> None:
    """Inherited on_platform_message should forward accepted messages."""

    adapter = build_adapter()
    sink = adapter.core_sink
    assert isinstance(sink, DummySink)
    asyncio.run(
        adapter.on_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
                "hop": 0,
                "ttl": 4,
                "message_id": "m-pipeline",
                "conversation_id": "c-pipeline",
                "trace_id": "t-pipeline",
                "payload": {"text": "请帮我确认一下"},
            }
        )
    )
    assert len(sink.captured) == 1


def test_adapter_health_check_uses_mqtt_client_state() -> None:
    """MQTT health should use paho client state."""

    adapter = build_adapter()
    assert asyncio.run(adapter.health_check()) is False
    adapter._mqtt_client = StubClient(True)
    assert asyncio.run(adapter.health_check()) is True
    adapter._mqtt_client = StubClient(False)
    adapter._reconnecting = True
    assert asyncio.run(adapter.health_check()) is True


def test_adapter_reconnect_does_not_stop_mqtt_loop() -> None:
    """Framework reconnect should not tear down the paho client."""

    adapter = build_adapter()
    adapter._mqtt_client = object()
    asyncio.run(adapter.reconnect())
    assert adapter._mqtt_client is not None


def test_adapter_stops_existing_mqtt_client_before_reconnect() -> None:
    """Reconnect setup should not leak old paho network loops."""

    adapter = build_adapter()
    client = StubClient()
    adapter._mqtt_client = client
    adapter._stop_mqtt_client()
    assert client.loop_stopped is True
    assert client.disconnected is True
    assert adapter._mqtt_client is None


def test_adapter_partner_resolution_handles_malformed_context() -> None:
    """Malformed relay_context should fall back to the first partner."""

    adapter = build_adapter()
    partner = adapter._resolve_partner_from_message_envelope(
        {
            "message_info": {
                "platform": "mqtt",
                "extra": {"relay_context": []},
            },
            "message_segment": [],
            "raw_message": {},
        }
    )
    assert partner.bot_id == "114514"

    partner = adapter._resolve_partner_from_message_envelope(
        {
            "message_info": {
                "platform": "mqtt",
                "extra": {"relay_context": {"peer_bot_id": ""}},
            },
            "message_segment": [],
            "raw_message": {},
        }
    )
    assert partner.bot_id == "114514"
