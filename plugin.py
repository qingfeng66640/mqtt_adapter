"""Standalone MQTT adapter plugin."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlparse

from mofox_wire import CoreSink, MessageEnvelope

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseAdapter, BasePlugin, register_plugin
from src.kernel.concurrency import get_task_manager

from . import store
from .config import MqttAdapterConfig, PartnerSection
from .envelope import RelayEnvelope
from .policy import PolicyEngine
from .presence import PresenceManager
from .session import SessionManager
from .system_handler import SystemChannelHandler

logger = get_logger("mqtt_adapter")


def _validate_bot_identity(config: MqttAdapterConfig) -> None:
    """Validate MQTT adapter bot identity config."""

    bot_id = str(config.mqtt.bot_id).strip()
    bot_name = str(config.mqtt.bot_name).strip()
    invalid_values = {"", "0", "none", "null", "undefined", "pydanticundefined"}
    if bot_id.lower() in invalid_values:
        raise ValueError("配置项 mqtt.bot_id 无效：必须为非空字符串")
    if bot_name.lower() in invalid_values:
        raise ValueError("配置项 mqtt.bot_name 无效：必须为非空名称")


class MqttAdapter(BaseAdapter):
    """Adapter exposing a bot-to-bot MQTT relay platform."""

    adapter_name = "mqtt_adapter"
    adapter_version = "0.1.0"
    adapter_author = "MoFox Team"
    adapter_description = "MQTT relay adapter"
    platform = "mqtt"

    _HEARTBEAT_INTERVAL = 30
    _RECONNECT_MIN_DELAY = 10
    _RECONNECT_MAX_DELAY = 120
    _KEEPALIVE = 20

    def __init__(self, core_sink: CoreSink, plugin: "MqttAdapterPlugin | None" = None, **kwargs: Any) -> None:
        """Initialize MQTT adapter runtime state."""

        super().__init__(core_sink, plugin=plugin, **kwargs)
        self._mqtt_client: Any | None = None
        self._mqtt_task_info: Any | None = None
        self._heartbeat_task_info: Any | None = None
        self._reconnect_task_info: Any | None = None
        self._reconnecting: bool = False
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._session_manager = SessionManager()
        self._policy_engine = PolicyEngine()
        self._reconnect_delay = self._RECONNECT_MIN_DELAY

    @property
    def mqtt_config(self) -> MqttAdapterConfig:
        """Return typed plugin config."""

        if not self.plugin or not isinstance(self.plugin.config, MqttAdapterConfig):
            raise RuntimeError("MQTT adapter requires MqttAdapterConfig")
        return self.plugin.config

    async def on_adapter_loaded(self) -> None:
        """Start MQTT background connection task via task_manager."""

        if not self.mqtt_config.plugin.enabled:
            logger.info("MQTT adapter disabled by config")
            return
        _validate_bot_identity(self.mqtt_config)
        self._event_loop = asyncio.get_running_loop()
        tm = get_task_manager()
        self._mqtt_task_info = tm.create_task(
            self._mqtt_connect_loop(),
            name="mqtt_adapter_mqtt",
            daemon=True,
        )

    async def on_adapter_unloaded(self) -> None:
        """Publish offline presence and stop MQTT background tasks."""

        await self._publish_presence("offline")
        for task_info in (self._mqtt_task_info, self._heartbeat_task_info):
            if task_info:
                get_task_manager().cancel_task(task_info.task_id)
        self._mqtt_task_info = None
        self._heartbeat_task_info = None
        self._stop_mqtt_client()

    def _cancel_heartbeat_task(self) -> None:
        """Cancel the current heartbeat task if one is registered."""

        if self._heartbeat_task_info:
            get_task_manager().cancel_task(self._heartbeat_task_info.task_id)
            self._heartbeat_task_info = None

    def _stop_mqtt_client(self) -> None:
        """Stop the existing paho client without publishing presence."""

        if self._mqtt_client is None:
            return
        loop_stop = getattr(self._mqtt_client, "loop_stop", None)
        if callable(loop_stop):
            loop_stop()
        disconnect = getattr(self._mqtt_client, "disconnect", None)
        if callable(disconnect):
            disconnect()
        self._mqtt_client = None

    async def health_check(self) -> bool:
        """Report MQTT client health instead of BaseAdapter transport health."""

        if self._mqtt_client is None:
            return False
        is_connected = getattr(self._mqtt_client, "is_connected", None)
        if callable(is_connected):
            return bool(is_connected()) or self._reconnecting
        return True

    async def reconnect(self) -> None:
        """Let the MQTT disconnect callback own reconnect scheduling."""

        logger.debug("MQTT reconnect is managed by paho disconnect callbacks")

    def _parse_broker_url(self) -> tuple[str, int]:
        """Parse host and port from broker_url."""

        parsed = urlparse(self.mqtt_config.mqtt.broker_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 1883
        return host, port

    async def _mqtt_connect_loop(self) -> None:
        """Connect to MQTT broker, subscribe topics, and start heartbeat."""

        try:
            import paho.mqtt.client as mqtt
        except Exception as error:  # pragma: no cover
            logger.warning(f"paho-mqtt unavailable in current environment: {error}")
            return

        config = self.mqtt_config.mqtt
        broker_host, broker_port = self._parse_broker_url()
        self._cancel_heartbeat_task()
        self._stop_mqtt_client()

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"mqtt_adapter_{config.bot_id}",
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        client.on_connect = self._on_mqtt_connect
        client.on_message = self._on_mqtt_message_callback
        client.on_disconnect = self._on_mqtt_disconnect
        if config.auth_token:
            username_pw_set = getattr(client, "username_pw_set", None)
            if callable(username_pw_set):
                username_pw_set(username=config.bot_id, password=config.auth_token)

        presence_mgr = PresenceManager(self.mqtt_config)
        will_envelope = presence_mgr.build_presence_envelope(status="offline")
        will_payload = json.dumps(will_envelope.to_dict(), ensure_ascii=False)
        client.will_set(f"bot/presence/{config.bot_id}", will_payload, qos=1, retain=True)

        logger.info(f"MQTT adapter connecting to {broker_host}:{broker_port}")
        try:
            client.connect(broker_host, broker_port, keepalive=self._KEEPALIVE)
        except Exception as exc:
            logger.warning(f"MQTT connect failed: {exc}; will retry")
            self._reconnect_delay = min(self._reconnect_delay * 2, self._RECONNECT_MAX_DELAY)
            await asyncio.sleep(self._reconnect_delay)
            self._mqtt_task_info = get_task_manager().create_task(
                self._mqtt_connect_loop(),
                name="mqtt_adapter_mqtt",
                daemon=True,
            )
            return

        client.loop_start()
        self._mqtt_client = client
        self._reconnect_delay = self._RECONNECT_MIN_DELAY
        self._heartbeat_task_info = get_task_manager().create_task(
            self._heartbeat_loop(client, config.bot_id),
            name="mqtt_adapter_heartbeat",
            daemon=True,
        )

    def _on_mqtt_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """Handle successful MQTT broker connection."""

        _ = userdata, flags, properties
        is_failure = getattr(reason_code, "is_failure", None)
        ok = not is_failure() if callable(is_failure) else reason_code == 0
        if ok:
            logger.info("MQTT adapter connected")
            config = self.mqtt_config.mqtt
            client.subscribe(f"bot/{config.bot_id}/inbox", qos=1)
            logger.info(f"Subscribed to bot/{config.bot_id}/inbox")
            for partner_bot_id in self.mqtt_config.presence.allowed_partner_bots:
                client.subscribe(f"bot/presence/{partner_bot_id}", qos=1)
                logger.info(f"Subscribed to bot/presence/{partner_bot_id}")
            self._publish_presence_sync(client, config.bot_id, "online")
        else:
            logger.warning(f"MQTT connect returned reason_code: {reason_code}")

    def _on_mqtt_disconnect(
        self,
        client: Any,
        userdata: Any,
        disconnect_flags: Any = None,
        reason_code: Any = None,
        properties: Any = None,
    ) -> None:
        """Schedule reconnect after MQTT disconnect."""

        _ = client, userdata, disconnect_flags, properties
        logger.info(f"MQTT disconnected (reason_code={reason_code})")
        if self._reconnecting:
            logger.debug("Reconnect already in flight; skipping duplicate schedule")
            return
        if self._event_loop is None or self._event_loop.is_closed():
            logger.warning("Disconnect callback fired without an event loop; cannot reconnect")
            return
        self._reconnecting = True
        self._reconnect_delay = min(self._reconnect_delay * 2, self._RECONNECT_MAX_DELAY)
        asyncio.run_coroutine_threadsafe(self._mqtt_reconnect_delayed(), self._event_loop)

    async def _mqtt_reconnect_delayed(self) -> None:
        """Sleep then retry connection and clear reconnect flag."""

        try:
            await asyncio.sleep(self._reconnect_delay)
            self._mqtt_task_info = get_task_manager().create_task(
                self._mqtt_connect_loop(),
                name="mqtt_adapter_mqtt",
                daemon=True,
            )
        finally:
            self._reconnecting = False
            self._reconnect_task_info = None

    def _on_mqtt_message_callback(self, client: Any, userdata: Any, msg: Any) -> None:
        """Schedule inbound MQTT message processing on the captured event loop."""

        _ = client, userdata
        try:
            raw = msg.payload.decode("utf-8")
        except Exception as exc:
            logger.warning(f"MQTT message decode failed on topic {msg.topic}: {exc}")
            return
        log_message = f"MQTT inbound on {msg.topic} ({len(raw)} bytes); dispatching to event loop"
        if str(msg.topic).startswith("bot/presence/"):
            if self.mqtt_config.mqtt.show_system_message_logs:
                logger.info(log_message)
        else:
            logger.info(log_message)
        if self._event_loop is None or self._event_loop.is_closed():
            logger.warning("MQTT message received but event loop unavailable; dropping")
            return
        asyncio.run_coroutine_threadsafe(self.on_platform_message(raw), self._event_loop)

    async def _publish_presence(self, status: str) -> None:
        """Publish retained presence message."""

        if self._mqtt_client is None:
            return
        presence_mgr = PresenceManager(self.mqtt_config)
        envelope = presence_mgr.build_presence_envelope(status=status)
        topic = self._topic_for_envelope(envelope)
        payload = json.dumps(envelope.to_dict(), ensure_ascii=False)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=True)

    @staticmethod
    def _publish_presence_sync(client: Any, bot_id: str, status: str) -> None:
        """Publish presence synchronously from MQTT callback thread."""

        payload = json.dumps(
            {
                "from_bot": bot_id,
                "to_bot": "*",
                "channel": "system",
                "intent": "presence_update",
                "terminal": True,
                "expect_reply": False,
                "payload": {"status": status},
            },
            ensure_ascii=False,
        )
        publish = getattr(client, "publish", None)
        if callable(publish):
            publish(f"bot/presence/{bot_id}", payload, qos=1, retain=True)

    async def _heartbeat_loop(self, client: Any, bot_id: str) -> None:
        """Publish presence periodically to signal online status."""

        while True:
            try:
                self._publish_presence_sync(client, bot_id, "online")
                await asyncio.sleep(self._HEARTBEAT_INTERVAL)
            except Exception:
                await asyncio.sleep(5)

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """Return local bot identity."""

        return {
            "bot_id": self.mqtt_config.mqtt.bot_id,
            "bot_name": self.mqtt_config.mqtt.bot_name,
            "platform": self.platform,
        }

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """Translate MessageEnvelope into RelayEnvelope and publish via MQTT."""

        partner = self._resolve_partner_from_message_envelope(envelope)
        relay_envelope = self._session_manager.build_outbound_envelope(
            message_envelope=envelope,
            from_bot=self.mqtt_config.mqtt.bot_id,
            from_bot_name=self.mqtt_config.mqtt.bot_name,
            to_bot=partner.bot_id,
            to_bot_name=partner.bot_name,
            default_ttl=self.mqtt_config.mqtt.default_ttl,
            default_reply_budget=self.mqtt_config.mqtt.default_reply_budget,
        )
        relay_envelope = self._policy_engine.apply_outbound(relay_envelope)
        relay_envelope.validate()
        await self.publish_relay_envelope(relay_envelope)

    async def publish_relay_envelope(self, envelope: RelayEnvelope) -> None:
        """Publish a validated relay envelope through the current MQTT client."""

        if self._mqtt_client is None:
            logger.info("MQTT client not connected; skipping live publish in current environment")
            return
        payload = json.dumps(envelope.to_dict(), ensure_ascii=False)
        topic = self._topic_for_envelope(envelope)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=False)

    async def from_platform_message(self, raw: Any) -> MessageEnvelope | None:  # type: ignore[override]
        """Convert raw relay payload into MessageEnvelope or consume system events."""

        if isinstance(raw, str):
            raw_dict = json.loads(raw)
        elif isinstance(raw, bytes):
            raw_dict = json.loads(raw.decode("utf-8"))
        elif isinstance(raw, dict):
            raw_dict = raw
        else:
            return None
        relay_envelope = RelayEnvelope.from_dict(raw_dict)
        relay_envelope = relay_envelope.increment_hop()
        relay_envelope.validate()
        presence_manager = PresenceManager(self.mqtt_config)
        if relay_envelope.to_bot not in {self.mqtt_config.mqtt.bot_id, "*"}:
            logger.warning(f"Ignoring relay envelope for different target bot: {relay_envelope.to_bot}")
            return None
        if relay_envelope.channel != "system" and not presence_manager.is_allowed(relay_envelope.from_bot):
            logger.warning(
                "Rejecting relay envelope from unknown partner bot: "
                f"from_bot={relay_envelope.from_bot}, conversation_id={relay_envelope.conversation_id}"
            )
            store.audit(
                "sender_not_allowed",
                from_bot=relay_envelope.from_bot,
                to_bot=relay_envelope.to_bot,
                channel=relay_envelope.channel,
                intent=relay_envelope.intent,
                conversation_id=relay_envelope.conversation_id,
            )
            await self._publish_sender_not_allowed_error(relay_envelope)
            return None
        system_handler = SystemChannelHandler(presence_manager)
        if system_handler.handle(relay_envelope):
            return None
        if self._is_orphan_transaction_continuation(relay_envelope):
            logger.warning(
                "Dropping orphan relay transaction continuation: "
                f"from_bot={relay_envelope.from_bot}, conversation_id={relay_envelope.conversation_id}, "
                f"intent={relay_envelope.intent}"
            )
            store.audit(
                "orphan_transaction_continuation",
                from_bot=relay_envelope.from_bot,
                to_bot=relay_envelope.to_bot,
                intent=relay_envelope.intent,
                conversation_id=relay_envelope.conversation_id,
            )
            return None
        transaction_session = self._session_manager.sync_inbound_transaction_session(relay_envelope)
        if transaction_session is not None:
            self._apply_session_state_to_envelope(relay_envelope, transaction_session)
        self._session_manager.sync_inbound_social_session(relay_envelope)
        return MessageEnvelope(
            direction="incoming",
            message_info={
                "platform": self.platform,
                "message_id": relay_envelope.message_id,
                "message_type": "message",
                "user_info": {
                    "platform": self.platform,
                    "user_id": relay_envelope.from_bot,
                    "user_nickname": relay_envelope.from_bot_name,
                },
                "extra": {
                    "bot_internal": True,
                    "relay_context": self._session_manager.relay_context_from_envelope(relay_envelope),
                    "relay_envelope": relay_envelope.to_dict(),
                },
            },
            message_segment=[{"type": "text", "data": relay_envelope.text}],
            raw_message=raw_dict,
        )

    @staticmethod
    def _is_orphan_transaction_continuation(envelope: RelayEnvelope) -> bool:
        """Reject transaction follow-ups that have no local session."""

        if envelope.channel != "transaction" or envelope.intent in {"notify", "request", "invite"}:
            return False
        return store.get_session(envelope.conversation_id) is None

    async def _publish_sender_not_allowed_error(self, inbound: RelayEnvelope) -> None:
        """Send an explicit protocol error for rejected non-system envelopes."""

        error_envelope = RelayEnvelope(
            conversation_id=inbound.conversation_id,
            trace_id=inbound.trace_id,
            parent_message_id=inbound.message_id,
            from_bot=self.mqtt_config.mqtt.bot_id,
            from_bot_name=self.mqtt_config.mqtt.bot_name,
            to_bot=inbound.from_bot,
            to_bot_name=inbound.from_bot_name,
            channel="system",
            intent="error",
            expect_reply=False,
            reply_budget=0,
            ttl=self.mqtt_config.mqtt.default_ttl,
            terminal=True,
            allowed_responders=[],
            no_relay=True,
            payload={
                "code": "sender_not_allowed",
                "text": "Sender bot is not allowed to contact this MQTT relay endpoint.",
                "rejected_channel": inbound.channel,
                "rejected_intent": inbound.intent,
            },
        )
        try:
            error_envelope.validate()
            await self.publish_relay_envelope(error_envelope)
        except Exception as exc:
            logger.error(
                "Failed to publish sender-not-allowed relay error: "
                f"from_bot={inbound.from_bot}, conversation_id={inbound.conversation_id}, error={exc}",
                exc_info=True,
            )

    @staticmethod
    def _apply_session_state_to_envelope(envelope: RelayEnvelope, session: store.RelaySession) -> None:
        """Reflect locally applied session state in downstream relay_context."""

        envelope.state = session.state
        envelope.terminal = session.terminal
        envelope.expect_reply = session.expect_reply
        envelope.reply_budget = session.reply_budget
        envelope.allowed_responders = list(session.allowed_responders)

    def _resolve_partner_from_message_envelope(self, envelope: MessageEnvelope) -> PartnerSection:
        """Resolve the routing partner from envelope metadata."""

        message_info = envelope.get("message_info") if isinstance(envelope, dict) else None
        extra = message_info.get("extra") if isinstance(message_info, dict) else None
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
        peer_bot_id = relay_context.get("peer_bot_id") if isinstance(relay_context, dict) else None
        if isinstance(peer_bot_id, str) and peer_bot_id:
            partner = self.mqtt_config.partner_by_id(peer_bot_id)
            if partner is not None:
                return partner
        partner = self.mqtt_config.first_allowed_partner()
        if partner is None or not partner.bot_id:
            raise ValueError("No allowed MQTT relay partner configured")
        return partner

    def _topic_for_envelope(self, envelope: RelayEnvelope) -> str:
        """Return MQTT topic for a relay envelope."""

        if envelope.channel == "system" and envelope.intent == "presence_update":
            return f"bot/presence/{envelope.from_bot}"
        return f"bot/{envelope.to_bot}/inbox"


@register_plugin
class MqttAdapterPlugin(BasePlugin):
    """Standalone MQTT relay adapter plugin."""

    plugin_name = "mqtt_adapter"
    plugin_version = "0.1.0"
    plugin_author = "MoFox Team"
    plugin_description = "MQTT relay adapter for Neo-MoFox"
    configs = [MqttAdapterConfig]
    dependent_components: list[str] = []

    def get_components(self) -> list[type]:
        """Return plugin component classes."""

        return [MqttAdapter]
