"""独立 MQTT 适配器插件。

本模块实现了基于 MQTT 协议的 bot-to-bot 中继通信适配器，包含两个核心类:

- **MqttAdapter**: 适配器组件，负责 MQTT 连接生命周期管理、消息收发、
  在线状态发布、协议转换等。继承自 BaseAdapter，通过 CoreSink 与框架内核交互。
- **MqttAdapterPlugin**: 插件入口，向框架注册 MqttAdapter 组件和配置。

架构说明:
    MqttAdapter 不直接使用 asyncio.create_task()，而是通过框架的
    get_task_manager() 管理后台任务（MQTT 连接循环、心跳循环），
    确保任务生命周期与插件加载/卸载正确绑定。

MQTT 消息流::

    入站: MQTT Broker → paho callback → on_platform_message() → from_platform_message()
         → MessageEnvelope → CoreSink → chatter → ...

    出站: ... → _send_platform_message() → build_outbound_envelope()
         → PolicyEngine → publish_relay_envelope() → MQTT Broker

Topic 约定:
    - 入站消息: bot/{bot_id}/inbox
    - 在线状态: bot/presence/{bot_id}
"""

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
    """验证 MQTT 适配器的 bot 身份配置是否有效。

    在适配器启动前调用，确保 bot_id 和 bot_name 不是空值或占位符。
    如果配置无效，直接抛出异常阻止启动，避免以无效身份连接到 MQTT Broker。

    参数:
        config: MQTT 适配器配置实例。

    异常:
        ValueError: bot_id 或 bot_name 为无效值时抛出。
    """

    bot_id = str(config.mqtt.bot_id).strip()
    bot_name = str(config.mqtt.bot_name).strip()
    invalid_values = {"", "0", "none", "null", "undefined", "pydanticundefined"}
    if bot_id.lower() in invalid_values:
        raise ValueError("配置项 mqtt.bot_id 无效：必须为非空字符串")
    if bot_name.lower() in invalid_values:
        raise ValueError("配置项 mqtt.bot_name 无效：必须为非空名称")


class MqttAdapter(BaseAdapter):
    """通过 MQTT 协议暴露 bot-to-bot 中继通信的适配器。

    继承自 BaseAdapter（进而继承自 mofox_wire.AdapterBase），
    实现 _send_platform_message() 和 from_platform_message() 两个核心方法，
    完成 MessageEnvelope 与 RelayEnvelope 之间的双向转换。

    后台任务:
        - mqtt_adapter_mqtt: MQTT 连接循环，负责连接、重连和订阅
        - mqtt_adapter_heartbeat: 定期发布在线状态的心跳任务

    重连策略:
        使用指数退避（exponential backoff），初始延迟 10 秒，最大 120 秒。
        每次连接失败后延迟翻倍，成功连接后重置为最小值。
    """

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
        """初始化 MQTT 适配器的运行时状态。

        所有 MQTT 相关的资源（client、task）都在 on_adapter_loaded() 中
        延迟初始化，而非在构造时。这是因为构造时 event loop 可能尚未就绪。

        参数:
            core_sink: 框架内核的 sink 接口，用于将 MessageEnvelope 推入内核管道。
            plugin: 所属的 MqttAdapterPlugin 实例（可选）。
            **kwargs: 传递给父类 BaseAdapter 的额外参数。
        """

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
        """获取类型化的插件配置。

        从 self.plugin.config 中取出配置并断言其类型为 MqttAdapterConfig。
        这是所有配置访问的统一入口，避免在代码中反复写类型断言。

        返回:
            当前插件的 MqttAdapterConfig 实例。

        异常:
            RuntimeError: 如果 plugin 未设置或 config 类型不匹配。
        """

        if not self.plugin or not isinstance(self.plugin.config, MqttAdapterConfig):
            raise RuntimeError("MQTT adapter requires MqttAdapterConfig")
        return self.plugin.config

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def on_adapter_loaded(self) -> None:
        """适配器加载时启动 MQTT 后台连接任务。

        执行步骤:
        1. 检查 plugin.enabled 开关，如果禁用则直接返回
        2. 验证 bot 身份配置的有效性
        3. 捕获当前 event loop 的引用（供 MQTT 回调线程使用）
        4. 通过 task_manager 创建后台 MQTT 连接任务

        如果配置中 plugin.enabled 为 False，适配器完全不启动，
        不会建立任何 MQTT 连接。
        """

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
        """适配器卸载时执行清理。

        清理顺序:
        1. 发布离线 presence 消息（retained），通知伙伴本 bot 已下线
        2. 取消 MQTT 连接任务和心跳任务
        3. 停止并断开 paho MQTT client

        注意:
            发布离线消息是尽力而为的——如果 MQTT 连接已经断开，
            此步骤可能失败（静默忽略）。
        """

        await self._publish_presence("offline")
        for task_info in (self._mqtt_task_info, self._heartbeat_task_info):
            if task_info:
                get_task_manager().cancel_task(task_info.task_id)
        self._mqtt_task_info = None
        self._heartbeat_task_info = None
        self._stop_mqtt_client()

    def _cancel_heartbeat_task(self) -> None:
        """取消当前心跳任务（如果存在）。

        在重新建立 MQTT 连接前调用，确保不会有多个心跳任务并行运行。
        """

        if self._heartbeat_task_info:
            get_task_manager().cancel_task(self._heartbeat_task_info.task_id)
            self._heartbeat_task_info = None

    def _stop_mqtt_client(self) -> None:
        """停止并清理当前的 paho MQTT client。

        调用 client 的 loop_stop() 和 disconnect() 方法，
        然后将引用置为 None。不会发布离线 presence——
        离线消息应在调用此方法之前单独发布。

        注意:
            使用 getattr 进行鸭子类型检查，避免对 paho 的强类型依赖。
        """

        if self._mqtt_client is None:
            return
        loop_stop = getattr(self._mqtt_client, "loop_stop", None)
        if callable(loop_stop):
            loop_stop()
        disconnect = getattr(self._mqtt_client, "disconnect", None)
        if callable(disconnect):
            disconnect()
        self._mqtt_client = None

    # ------------------------------------------------------------------
    # 健康检查与重连
    # ------------------------------------------------------------------

    async def health_check(self) -> bool:
        """报告 MQTT client 的连接健康状态。

        覆盖 BaseAdapter 的默认健康检查，不使用 transport 层级的健康检测，
        而是直接查询 paho client 的 is_connected() 状态。
        如果正在重连过程中（_reconnecting 为 True），也视为健康——
        因为重连逻辑已经在运行中。

        返回:
            True 表示 MQTT 连接正常或在重连中，False 表示 client 为 None。
        """

        if self._mqtt_client is None:
            return False
        is_connected = getattr(self._mqtt_client, "is_connected", None)
        if callable(is_connected):
            return bool(is_connected()) or self._reconnecting
        return True

    async def reconnect(self) -> None:
        """框架级别的重连请求入口。

        本适配器不在此处主动重连——MQTT 断线回调 (_on_mqtt_disconnect)
        已经负责调度重连。此处只记录日志，避免重复触发重连逻辑。
        如需强制重连，应通过配置热重载触发 on_adapter_unloaded/loaded 循环。

        注意:
            此方法不会停止当前的 MQTT client，因为 client 可能仍在工作。
        """

        logger.debug("MQTT reconnect is managed by paho disconnect callbacks")

    # ------------------------------------------------------------------
    # MQTT 连接管理
    # ------------------------------------------------------------------

    def _parse_broker_url(self) -> tuple[str, int]:
        """从 broker_url 中解析主机名和端口。

        使用标准库 urllib.parse.urlparse 解析 URL，支持 mqtt:// 和 mqtts:// 协议。
        默认值: host = "localhost", port = 1883。

        返回:
            (host, port) 元组。
        """

        parsed = urlparse(self.mqtt_config.mqtt.broker_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 1883
        return host, port

    async def _mqtt_connect_loop(self) -> None:
        """MQTT 连接循环：连接 broker、订阅 topic、启动心跳。

        执行流程:
        1. 导入 paho-mqtt（如果不可用则记录警告并退出）
        2. 取消旧的心跳任务，停止旧的 MQTT client
        3. 创建新的 paho Client 实例，注册回调
        4. 设置 Last Will (LWT)：离线时自动发布 retained 离线消息
        5. 尝试连接 broker；如果失败则指数退避后重新调度自己
        6. 连接成功后启动 client.loop_start() 和心跳任务

        此方法通过 task_manager 调度，自身也是一个可重入的协程——
        连接失败时会重新创建自身为新任务，形成连接-重连循环。
        """

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

        # 设置遗嘱消息 (Last Will and Testament)
        # 当 MQTT 连接异常断开时，broker 会自动向 presence topic 发布此消息
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

    # ------------------------------------------------------------------
    # MQTT 回调（在 paho 网络线程中执行，不能直接调用 asyncio API）
    # ------------------------------------------------------------------

    def _on_mqtt_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """MQTT 连接成功的回调（运行在 paho 网络线程）。

        在连接成功后:
        1. 检查 reason_code 确认连接状态
        2. 订阅本 bot 的 inbox topic: bot/{bot_id}/inbox
        3. 订阅所有白名单伙伴的 presence topic: bot/presence/{partner_bot_id}
        4. 同步发布本 bot 的在线 presence

        注意:
            此回调在 paho 的网络线程中执行，因此只能调用同步方法。
            不能直接调用 asyncio API。

        参数:
            client: paho MQTT client 实例。
            userdata: 用户自定义数据（未使用）。
            flags: 连接标志（未使用）。
            reason_code: 连接结果码（paho v2 API 返回 ReasonCode 对象）。
            properties: 连接属性（未使用）。
        """

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
        """MQTT 断开连接的回调（运行在 paho 网络线程）。

        在连接断开后:
        1. 记录断开日志
        2. 如果已有重连任务在进行中则跳过（防止重复调度）
        3. 如果 event_loop 不可用则放弃重连
        4. 通过 asyncio.run_coroutine_threadsafe() 将重连协程调度到主 event loop

        使用指数退避策略: 延迟时间每次翻倍，最大到 _RECONNECT_MAX_DELAY (120s)。

        参数:
            client: paho MQTT client 实例。
            userdata: 用户自定义数据（未使用）。
            disconnect_flags: 断开连接标志（未使用）。
            reason_code: 断开原因码。
            properties: 断开连接属性（未使用）。
        """

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
        """延迟重连协程（运行在主 event loop）。

        在指定的延迟后重新创建 MQTT 连接任务，并在 finally 中
        清除重连标志。这确保无论重连是否成功，_reconnecting 标志
        都能被正确重置。
        """

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
        """MQTT 消息到达回调（运行在 paho 网络线程）。

        处理流程:
        1. 解码消息载荷（UTF-8）
        2. 根据 topic 类型和配置决定日志级别
        3. 通过 asyncio.run_coroutine_threadsafe() 将消息调度到主 event loop
        4. 调用 on_platform_message() 进入标准的适配器消息管道

        关于日志控制:
            presence topic (bot/presence/*) 可能非常频繁（每 30 秒一次心跳），
            可以通过 show_system_message_logs 配置项控制是否打印这些日志。

        注意:
            此回调在 paho 网络线程中执行，不能直接调用协程。
            必须使用 run_coroutine_threadsafe 将消息投递到主 event loop。

        参数:
            client: paho MQTT client 实例（未使用）。
            userdata: 用户自定义数据（未使用）。
            msg: paho MQTTMessage 对象，包含 topic 和 payload 属性。
        """

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

    # ------------------------------------------------------------------
    # 在线状态
    # ------------------------------------------------------------------

    async def _publish_presence(self, status: str) -> None:
        """异步发布 retained 在线状态消息。

        通过当前的 MQTT client 发布一条 presence_update 系统消息。
        使用 retained=True 确保新上线的 bot 能立即获知本 bot 的状态。

        参数:
            status: 在线状态字符串（"online" 或 "offline"）。
        """

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
        """同步发布在线状态（用于 MQTT 回调线程中调用）。

        直接在 paho 网络线程中发布，无需经过 event loop。
        在 _on_mqtt_connect 和 _heartbeat_loop 中被调用。

        参数:
            client: paho MQTT client 实例。
            bot_id: 本 bot 的路由 ID。
            status: 在线状态字符串。
        """

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
        """心跳循环：定期发布在线状态。

        每 _HEARTBEAT_INTERVAL 秒（默认 30 秒）发布一次 online presence，
        告知伙伴 bot 本 bot 仍在正常运行。如果发布过程中发生异常，
        短暂等待 5 秒后继续尝试，避免错误循环中疯狂重试。

        参数:
            client: paho MQTT client 实例。
            bot_id: 本 bot 的路由 ID。
        """

        while True:
            try:
                self._publish_presence_sync(client, bot_id, "online")
                await asyncio.sleep(self._HEARTBEAT_INTERVAL)
            except Exception:
                await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # 身份信息
    # ------------------------------------------------------------------

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """返回本地 bot 的身份信息。

        覆盖 BaseAdapter.get_bot_info()，提供 MQTT 适配器特有的身份字段。
        返回的字典用于框架统一身份查询和日志记录。

        返回:
            包含 bot_id、bot_name 和 platform 的字典。
        """

        return {
            "bot_id": self.mqtt_config.mqtt.bot_id,
            "bot_name": self.mqtt_config.mqtt.bot_name,
            "platform": self.platform,
        }

    # ------------------------------------------------------------------
    # 消息协议转换（核心）
    # ------------------------------------------------------------------

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """将 MessageEnvelope 转换为 RelayEnvelope 并通过 MQTT 发布。

        这是出站消息的核心转换方法，被框架调用以将 bot 的回复
        发送给伙伴 bot。转换流程:

        1. 从 envelope 中解析目标伙伴（_resolve_partner_from_message_envelope）
        2. 通过 SessionManager 构建 RelayEnvelope（含事务/社交会话状态）
        3. 通过 PolicyEngine 应用 outbound 策略规则
        4. 校验信封的合法性
        5. 通过 MQTT publish 发送

        参数:
            envelope: 框架层传递的 MessageEnvelope，包含消息段和元数据。
        """

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
        """通过当前 MQTT client 发布一条已验证的中继信封。

        使用 QoS 1（至少一次送达），不保留消息（retain=False）。
        如果 MQTT client 为 None（未连接），记录日志并静默跳过——
        不会抛出异常，因为这是异步发布中的正常边界情况。

        参数:
            envelope: 已验证合法的 RelayEnvelope。
        """

        if self._mqtt_client is None:
            logger.info("MQTT client not connected; skipping live publish in current environment")
            return
        payload = json.dumps(envelope.to_dict(), ensure_ascii=False)
        topic = self._topic_for_envelope(envelope)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=False)

    async def from_platform_message(self, raw: Any) -> MessageEnvelope | None:  # type: ignore[override]
        """将原始中继载荷转换为 MessageEnvelope 或消费系统事件。

        这是入站消息的核心转换方法。处理流程:

        1. **解析**: 支持 str、bytes、dict 三种原始格式，统一转为 dict
        2. **反序列化**: 通过 RelayEnvelope.from_dict() 构造信封
        3. **hop 递增**: 调用 increment_hop() 增加跳数（TTL 保护）
        4. **校验**: 调用 validate() 检查必须字段和 hop/ttl 关系
        5. **目标检查**: 验证 to_bot 是否匹配本 bot（或通配符 "*"）
        6. **权限检查**: 非系统消息需要发送方在白名单中
        7. **系统消息短路**: 系统信道消息由 SystemChannelHandler 消费
        8. **孤儿消息过滤**: 不属于任何已知会话的事务后续消息被丢弃
        9. **会话同步**: 更新 transaction/social 会话状态
        10. **构建 MessageEnvelope**: 将 RelayEnvelope 转换为框架标准格式

        返回 None 表示消息被过滤（权限拒绝、目标不匹配、孤儿消息等），
        框架不会进一步处理。

        参数:
            raw: 原始消息数据，可以是 JSON 字符串、bytes 或已解析的 dict。

        返回:
            转换后的 MessageEnvelope，如果消息应被过滤则返回 None。
        """

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
        """判断是否为孤儿事务后续消息。

        孤儿消息是指 intent 为 accept/decline/confirm 等事务后续操作，
        但本地 store 中不存在对应会话记录的消息。这通常发生在:
        - 会话已在本地被清理
        - 对方 bot 使用了错误的 conversation_id
        - 消息到达延迟导致会话已过期

        不会被视为孤儿的 intent: notify、request、invite（这些是会话的起点）。

        参数:
            envelope: 待检查的中继信封。

        返回:
            True 表示该消息是孤儿事务后续消息，应被丢弃。
        """

        if envelope.channel != "transaction" or envelope.intent in {"notify", "request", "invite"}:
            return False
        return store.get_session(envelope.conversation_id) is None

    async def _publish_sender_not_allowed_error(self, inbound: RelayEnvelope) -> None:
        """向被拒绝的发送方发布 sender_not_allowed 错误。

        当非白名单 bot 尝试发送非系统消息时，本 bot 会回复一条系统信道的
        error 消息，告知对方其不被允许通信。错误信封标记为:
        - terminal=True: 不期待回复
        - no_relay=True: 通知中继节点不要继续转发此错误

        参数:
            inbound: 被拒绝的入站信封，用于提取 from_bot、conversation_id 等信息。
        """

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
        """将本地会话状态反映到信封的 relay_context 中。

        在将信封转换为 MessageEnvelope 之前，用 SessionManager
        更新后的会话状态覆盖信封的对应字段。这确保下游组件
        （chatter 等）能看到最新的会话状态（state、terminal 等）。

        参数:
            envelope: 原始入站信封（会被原地修改）。
            session: SessionManager 同步后的最新会话状态。
        """

        envelope.state = session.state
        envelope.terminal = session.terminal
        envelope.expect_reply = session.expect_reply
        envelope.reply_budget = session.reply_budget
        envelope.allowed_responders = list(session.allowed_responders)

    def _resolve_partner_from_message_envelope(self, envelope: MessageEnvelope) -> PartnerSection:
        """从 MessageEnvelope 的元数据中解析目标伙伴。

        解析优先级:
        1. envelope.message_info.extra.relay_context.peer_bot_id —— 显式指定的伙伴 ID
        2. 通过 config.partner_by_id() 查找匹配的 PartnerSection
        3. 回退到 config.first_allowed_partner() —— 白名单中的第一个伙伴

        参数:
            envelope: 出站 MessageEnvelope。

        返回:
            解析出的 PartnerSection。

        异常:
            ValueError: 如果没有任何可用伙伴配置。
        """

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
        """根据信封类型返回对应的 MQTT topic。

        Topic 规则:
        - presence_update 系统消息 → bot/presence/{from_bot}
          使用 from_bot 是因为 presence 以发布者身份命名
        - 其他所有消息 → bot/{to_bot}/inbox
          使用 to_bot 是因为消息目标是接收方的 inbox

        参数:
            envelope: 待发布的中继信封。

        返回:
            MQTT topic 字符串。
        """

        if envelope.channel == "system" and envelope.intent == "presence_update":
            return f"bot/presence/{envelope.from_bot}"
        return f"bot/{envelope.to_bot}/inbox"


@register_plugin
class MqttAdapterPlugin(BasePlugin):
    """独立 MQTT 中继适配器插件入口。

    负责向框架注册 MqttAdapter 组件和 MqttAdapterConfig 配置类。
    通过 @register_plugin 装饰器自动注册到插件系统。

    插件元数据:
        - plugin_name: "mqtt_adapter"（必须与 manifest.json 中的 name 和目录名一致）
        - plugin_version: 遵循语义化版本
        - configs: 声明此插件使用的配置类列表
        - dependent_components: 不依赖其他插件组件
    """

    plugin_name = "mqtt_adapter"
    plugin_version = "0.1.0"
    plugin_author = "MoFox Team"
    plugin_description = "MQTT relay adapter for Neo-MoFox"
    configs = [MqttAdapterConfig]
    dependent_components: list[str] = []

    def get_components(self) -> list[type]:
        """返回此插件提供的组件类列表。

        框架在加载插件时会调用此方法，将返回的组件类实例化并注册到
        对应的 manager 中（此处为 adapter_manager）。

        返回:
            包含 MqttAdapter 类的列表。
        """

        return [MqttAdapter]