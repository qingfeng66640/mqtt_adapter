"""MQTT 适配器插件的契约测试。

本测试模块覆盖 MQTT 适配器的核心契约，确保:
- 插件身份（manifest、plugin_name、adapter_name）与框架加载器期望一致
- 配置查找（partner_by_id、first_allowed_partner）正确路由
- RelayEnvelope 的序列化、校验和 TTL 保护机制
- store 模块的去重和状态重置
- PolicyEngine 的对通知消息强制 terminal 语义
- SessionManager 的外发信封构建和意图推断
- PresenceManager 和 SystemChannelHandler 的系统消息处理
- 适配器的入站消息过滤（错误目标、未知伙伴、孤儿消息）
- 适配器的健康检查和重连行为
"""

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
    """最小化的 CoreSink 桩，用于捕获适配器发送的 MessageEnvelope。

    不执行实际的 sink 逻辑，仅将收到的信封追加到 captured 列表中，
    供测试断言使用。
    """

    captured: list[MessageEnvelope]

    def __init__(self) -> None:
        """初始化捕获列表为空。"""

        self.captured = []

    async def send(self, envelope: MessageEnvelope) -> None:
        """捕获适配器发送的信封。

        参数:
            envelope: 适配器通过 sink 发送的 MessageEnvelope。
        """

        self.captured.append(envelope)


class DummyMqttMessage:
    """MQTT 消息的测试替身。

    模拟 paho MQTT 的 MQTTMessage 对象，包含 topic 和 payload 属性。
    payload 自动编码为 UTF-8 bytes，模拟真实 MQTT 消息的格式。
    """

    def __init__(self, topic: str, payload: str) -> None:
        """初始化消息的 topic 和编码后的 payload。

        参数:
            topic: MQTT topic 字符串。
            payload: 消息载荷（JSON 字符串），将被编码为 UTF-8 bytes。
        """

        self.topic = topic
        self.payload = payload.encode("utf-8")


class StubClient:
    """paho MQTT client 的测试替身。

    模拟 paho Client 的连接状态和方法调用记录，
    用于验证适配器的生命周期管理逻辑。
    """

    def __init__(self, connected: bool = True) -> None:
        """初始化连接状态和方法调用标志。

        参数:
            connected: 模拟的连接状态（is_connected() 的返回值）。
        """

        self.connected = connected
        self.loop_stopped = False
        self.disconnected = False

    def is_connected(self) -> bool:
        """返回配置的连接状态。"""

        return self.connected

    def loop_stop(self) -> None:
        """记录 loop_stop 调用。"""

        self.loop_stopped = True

    def disconnect(self) -> None:
        """记录 disconnect 调用。"""

        self.disconnected = True


def build_config() -> MqttAdapterConfig:
    """构建用于测试的有效 MQTT 适配器配置。

    配置包含:
    - 本 bot: bot_id="223123", bot_name="清风"
    - 伙伴 bot: bot_id="114514", bot_name="流光"
    - 白名单: allowed_partner_bots=["114514"]

    返回:
        预配置的 MqttAdapterConfig 实例。
    """

    config = MqttAdapterConfig()
    config.mqtt.bot_id = "223123"
    config.mqtt.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    return config


def build_adapter() -> MqttAdapter:
    """构建用于测试的 MqttAdapter 实例。

    使用 DummySink 作为 core_sink，避免依赖真实的框架内核。

    返回:
        配置好的 MqttAdapter 实例（未启动 MQTT 连接）。
    """

    plugin = MqttAdapterPlugin(build_config())
    return MqttAdapter(core_sink=DummySink(), plugin=plugin)


# ====================================================================
# 插件身份与 manifest
# ====================================================================


def test_manifest_and_plugin_identity() -> None:
    """验证 manifest.json 和插件类的身份标识与加载器期望一致。

    检查项:
    - manifest.name 与 plugin_name 一致（均为 "mqtt_adapter"）
    - python_dependencies 声明了 paho-mqtt>=2.0
    - include 中声明了 adapter 类型的 mqtt_adapter 组件
    - MqttAdapter 在 MqttAdapterPlugin.get_components() 返回列表中
    """

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


# ====================================================================
# 配置
# ====================================================================


def test_config_partner_lookup_uses_bot_id() -> None:
    """验证 partner_by_id() 仅基于 bot_id 进行路由匹配。

    测试内容:
    - 通过 bot_id="114514" 能正确找到名称为 "流光" 的伙伴
    - first_allowed_partner() 返回的正是这个伙伴
    """

    config = build_config()
    partner = config.partner_by_id("114514")
    assert partner is not None
    assert partner.bot_name == "流光"
    assert config.first_allowed_partner() is partner


def test_config_default_path_matches_framework_convention() -> None:
    """验证 get_default_path() 返回符合框架约定的路径。

    框架期望的配置路径为: config/plugins/{plugin_name}/config.toml
    设置 _plugin_ = "mqtt_adapter" 后，路径最后三段应为
    ("plugins", "mqtt_adapter", "config.toml")。
    """

    MqttAdapterConfig._plugin_ = "mqtt_adapter"
    try:
        path = MqttAdapterConfig.get_default_path()
        assert path is not None
        assert path.parts[-3:] == ("plugins", "mqtt_adapter", "config.toml")
    finally:
        if hasattr(MqttAdapterConfig, "_plugin_"):
            delattr(MqttAdapterConfig, "_plugin_")


# ====================================================================
# RelayEnvelope
# ====================================================================


def test_relay_envelope_roundtrip_and_validation() -> None:
    """验证 RelayEnvelope 的序列化往返和必须字段校验。

    测试流程:
    1. 创建信封 → to_dict() → from_dict() → validate()
    2. 验证 from_bot 和 text 在往返后值不变
    """

    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", payload={"text": "hi"})
    rebuilt = RelayEnvelope.from_dict(envelope.to_dict())
    rebuilt.validate()
    assert rebuilt.from_bot == "223123"
    assert rebuilt.text == "hi"


def test_relay_envelope_increment_hop() -> None:
    """验证 increment_hop() 返回新信封且原信封不变。

    hop 递增是 TTL 保护的核心——每次消息经过一个节点时 hop+1，
    确保不影响原始信封（不可变风格）。
    """

    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", hop=0, ttl=4)
    incremented = envelope.increment_hop()
    assert incremented.hop == 1
    assert envelope.hop == 0


def test_relay_envelope_hop_exceeds_ttl_validation() -> None:
    """验证 hop 超过 ttl 时 validate() 抛出 ValueError。

    当 hop > ttl 时意味着消息在 bot 间循环了太多次，
    应被拒绝以防止无限中继。
    """

    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", hop=5, ttl=4)
    try:
        envelope.validate()
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "hop" in str(exc).lower() or "ttl" in str(exc).lower()


# ====================================================================
# Store
# ====================================================================


def test_store_dedup_and_reset() -> None:
    """验证 store 的消息去重和状态重置功能。

    测试内容:
    - remember_message("m1") 首次调用返回 True（新消息）
    - 再次调用返回 False（重复消息）
    - reset_state() 后 DEDUP_CACHE 被清空
    """

    store.reset_state()
    assert store.remember_message("m1") is True
    assert store.remember_message("m1") is False
    store.reset_state()
    assert store.DEDUP_CACHE == {}


# ====================================================================
# PolicyEngine
# ====================================================================


def test_policy_notify_is_one_way() -> None:
    """验证 notify intent 被策略引擎强制转为单向终端消息。

    notify 类型的消息不应触发回复链，PolicyEngine.apply_outbound()
    应将其 terminal=True、expect_reply=False、reply_budget=0。
    即使原始信封的 expect_reply=True、reply_budget=9，
    策略引擎也应该覆盖这些值。
    """

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


# ====================================================================
# SessionManager
# ====================================================================


def test_session_manager_builds_request() -> None:
    """验证 SessionManager 正确构建外发 request 信封。

    intent="request" 的信封应:
    - expect_reply=True（期待回复）
    - allowed_responders 包含目标 bot（[to_bot]）
    """

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


# ====================================================================
# Presence 与 SystemChannelHandler
# ====================================================================


def test_presence_and_system_handler_short_path() -> None:
    """验证 presence_update 系统消息在进入 core 之前被消费。

    系统信道的 presence_update 应由 SystemChannelHandler 直接处理，
    不应进入框架的 chatter 管道。处理后 PRESENCE_TABLE 中应有对应记录。
    """

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
    """验证 presence 入站日志是否受 show_system_message_logs 配置控制。

    当 show_system_message_logs=True 时，presence topic 的消息应记录日志；
    当 show_system_message_logs=False 时，presence topic 的消息不应记录日志；
    但普通 inbox 消息始终应记录日志。
    """

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


# ====================================================================
# 适配器入站消息过滤
# ====================================================================


def test_adapter_rejects_wrong_target_or_unknown_partner() -> None:
    """验证适配器拒绝错误目标和未知伙伴的消息。

    测试两个场景:
    1. 消息的 to_bot 不是本 bot（"999999" ≠ "223123"）→ 返回 None
    2. 消息的 from_bot 不在白名单中（"777777"）→ 返回 None
    """

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
    """验证未知伙伴收到系统错误回复且不会产生会话状态变更。

    当白名单外的 bot 发送消息时:
    1. from_platform_message() 返回 None（消息不进入 core）
    2. 适配器向对方发布一条 sender_not_allowed 的 system error
    3. error 信封的 control 字段: terminal=True, expect_reply=False, no_relay=True
    4. 不会在 SESSION_TABLE 中创建任何会话
    5. AUDIT_LOG 中记录 sender_not_allowed 事件
    """

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
    """验证白名单伙伴的消息被正确接收并转换为 MessageEnvelope。

    转换后的 MessageEnvelope 应包含:
    - user_info.user_id == from_bot（伙伴 bot ID）
    - relay_context 中包含正确的 peer_bot_id、allowed_responders 等
    - store 中创建了对应的会话记录
    """

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
    """验证孤儿事务后续消息在进入 core 前被丢弃。

    当收到 accept/confirm 等事务后续消息但本地没有对应会话时，
    消息应被丢弃且不创建任何会话。AUDIT_LOG 中记录 orphan 事件。
    """

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
    """验证入站消息的 hop 被正确递增。

    每次消息经过一个 bot 节点时 hop 应 +1。测试验证
    从 from_platform_message() 返回的 MessageEnvelope 中
    relay_envelope 的 hop 比原始值大 1。
    """

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
    """验证 on_platform_message() 将接收的消息推送到 CoreSink。

    基类的 on_platform_message() 管线性应正常工作：
    原始消息 → from_platform_message() → MessageEnvelope → core_sink.send()
    """

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


# ====================================================================
# 健康检查与重连
# ====================================================================


def test_adapter_health_check_uses_mqtt_client_state() -> None:
    """验证 health_check() 使用 MQTT client 的连接状态。

    测试场景:
    - _mqtt_client 为 None → 返回 False（未连接）
    - _mqtt_client 存在且 is_connected()=True → 返回 True
    - _mqtt_client 存在且 is_connected()=False 但 _reconnecting=True → 返回 True（正在重连）
    """

    adapter = build_adapter()
    assert asyncio.run(adapter.health_check()) is False
    adapter._mqtt_client = StubClient(True)
    assert asyncio.run(adapter.health_check()) is True
    adapter._mqtt_client = StubClient(False)
    adapter._reconnecting = True
    assert asyncio.run(adapter.health_check()) is True


def test_adapter_reconnect_does_not_stop_mqtt_loop() -> None:
    """验证框架级别的 reconnect() 不会拆除 paho client。

    MQTT 重连由 paho 的 disconnect 回调管理，框架的 reconnect()
    只是记录日志，不应主动停止 client。
    """

    adapter = build_adapter()
    adapter._mqtt_client = object()
    asyncio.run(adapter.reconnect())
    assert adapter._mqtt_client is not None


def test_adapter_stops_existing_mqtt_client_before_reconnect() -> None:
    """验证 _stop_mqtt_client() 正确停止旧 client 的 loop 和连接。

    调用 _stop_mqtt_client() 后:
    - client.loop_stopped 应为 True
    - client.disconnected 应为 True
    - adapter._mqtt_client 应为 None
    """

    adapter = build_adapter()
    client = StubClient()
    adapter._mqtt_client = client
    adapter._stop_mqtt_client()
    assert client.loop_stopped is True
    assert client.disconnected is True
    assert adapter._mqtt_client is None


def test_adapter_partner_resolution_handles_malformed_context() -> None:
    """验证畸形 relay_context 时的伙伴解析回退逻辑。

    测试场景:
    1. relay_context 是 list（不是 dict）→ 回退到 first_allowed_partner()
    2. relay_context 中 peer_bot_id 为空字符串 → 回退到 first_allowed_partner()
    """

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