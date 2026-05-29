"""MQTT 中继的在线状态追踪与白名单校验。

PresenceManager 负责管理伙伴 bot 的在线状态，并提供基于配置的
访问控制检查。在线状态通过 MQTT 的 retained 消息机制实现——
每个 bot 在连接时发布 retained presence 消息，新上线的 bot
可以通过订阅 presence topic 立即获知伙伴的当前状态。
"""

from __future__ import annotations

import time

from . import store
from .config import MqttAdapterConfig
from .envelope import RelayEnvelope


class PresenceManager:
    """管理伙伴 bot 的在线状态和访问控制。

    使用模块级 store.PRESENCE_TABLE 存储状态，不依赖数据库。
    每次收到 presence_update 系统消息时，更新对应 bot 的记录。
    访问控制基于配置中的 allowed_partner_bots 白名单。
    """

    def __init__(self, config: MqttAdapterConfig) -> None:
        """初始化在线状态管理器。

        参数:
            config: MQTT 适配器配置实例，用于读取白名单和安全策略。
        """

        self.config = config

    def is_allowed(self, bot_id: str) -> bool:
        """检查指定 bot_id 是否被允许与本 bot 通信。

        检查逻辑取决于配置中的 require_known_partner 开关：
        - 如果 require_known_partner 为 False，允许所有 bot（不推荐）
        - 如果为 True，仅允许 presence.allowed_partner_bots 列表中的 bot

        参数:
            bot_id: 要检查的伙伴 bot 路由 ID。

        返回:
            True 表示允许通信，False 表示拒绝。
        """

        if not self.config.presence.require_known_partner:
            return True
        return bot_id in self.config.presence.allowed_partner_bots

    def update_from_envelope(self, envelope: RelayEnvelope) -> None:
        """从系统信道的 presence_update 信封更新在线状态。

        从信封的 payload 中提取 status 字段（默认为 "online"），
        创建或更新 store 中的 PresenceRecord。

        参数:
            envelope: 包含 presence_update 信息的系统信道信封。
        """

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
        """构建本 bot 的在线状态信封。

        生成一条系统信道的 presence_update 消息，目标为通配符 "*"，
        表示此消息面向所有订阅了 presence topic 的 bot 广播。
        信封标记为 terminal=True，不需要回复。

        参数:
            status: 在线状态字符串，通常为 "online" 或 "offline"。

        返回:
            构造好的 RelayEnvelope，可直接序列化后通过 MQTT 发布。
        """

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