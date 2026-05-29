"""中继协议信封模型及其序列化、校验工具。

RelayEnvelope 是 bot 间 MQTT 通信的核心数据结构，封装了一条中继消息的
全部协议字段。所有通过 MQTT 收发的消息都以 RelayEnvelope 的 JSON 字典形式
进行序列化和反序列化。

协议字段说明:
    - protocol_version: 协议版本号，用于后续兼容性扩展
    - message_id: 消息唯一 ID（UUID hex），用于去重
    - conversation_id: 会话 ID，同一对话链路中保持不变
    - trace_id: 追踪 ID，用于跨 bot 日志关联
    - parent_message_id: 父消息 ID，指向触发本条消息的上一条消息
    - from_bot / to_bot: 发送方和接收方的 bot 路由 ID
    - channel: 信道类型（system / transaction / social）
    - intent: 消息意图（notify / request / invite / accept / decline / ...）
    - hop / ttl: 当前跳数和最大跳数，防止无限循环中继
    - terminal: 是否为终端消息（不再期待回复）
    - expect_reply: 是否期望对端回复
    - reply_budget: 剩余回复配额
    - allowed_responders: 允许回复的 bot_id 列表
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import uuid4

RelayChannel = Literal["system", "transaction", "social"]


@dataclass(slots=True)
class RelayEnvelope:
    """bot 间通过 MQTT 交换的协议信封。

    所有字段均支持通过 ``from_dict()`` 从 JSON 字典构造，并通过 ``to_dict()``
    序列化回字典。使用 ``__slots__`` 优化内存占用，避免为每个实例创建 __dict__。

    生命周期::

        构造 → validate() → to_dict() → JSON → MQTT publish
        MQTT receive → JSON → from_dict() → increment_hop() → validate() → 处理
    """

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
        """从字典反序列化构造 RelayEnvelope。

        仅提取 dataclass 中已声明的字段，自动忽略字典中的多余字段。
        这样可以在协议升级时保持向后兼容——新增字段通过默认值兜底。

        参数:
            data: 包含信封字段的字典（通常来自 JSON 反序列化）。

        返回:
            一个新的 RelayEnvelope 实例，未在 data 中的字段使用默认值。
        """

        known = {field_name for field_name in cls.__dataclass_fields__}
        return cls(**{key: value for key, value in data.items() if key in known})

    def to_dict(self) -> dict[str, Any]:
        """将信封序列化为适合 JSON 编码的字典。

        所有列表和字典字段均进行浅拷贝，避免外部修改影响信封内部状态。

        返回:
            包含所有信封字段的字典，可直接传给 json.dumps()。
        """

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
        """从 payload 中提取文本内容。

        优先读取 payload["text"]，不存在时返回空字符串。
        如果 text 字段不是字符串类型则强制转换。

        返回:
            payload 中的文本内容字符串。
        """

        value = self.payload.get("text", "")
        return value if isinstance(value, str) else str(value)

    def validate(self) -> None:
        """校验信封的必要字段和约束条件。

        校验规则:
            - message_id 不能为空：每条消息必须有唯一标识
            - from_bot 不能为空：必须知道消息来源
            - to_bot 不能为空：必须有明确的消息目标
            - hop 不能超过 ttl：防止消息无限循环中继
            - reply_budget 不能为负数：预算只能为零或正数

        异常:
            ValueError: 任一校验规则不满足时抛出。
        """

        if not self.message_id:
            raise ValueError("message_id 不能为空，每条消息都需要一个唯一标识")
        if not self.from_bot:
            raise ValueError("from_bot 不能为空，需要知道消息的发送方")
        if not self.to_bot:
            raise ValueError("to_bot 不能为空，需要知道消息的接收方")
        if self.hop > self.ttl:
            raise ValueError(f"hop ({self.hop}) 超过了 ttl ({self.ttl})，消息可能陷入循环中继")
        if self.reply_budget < 0:
            raise ValueError("reply_budget 不能为负数")

    def increment_hop(self) -> "RelayEnvelope":
        """创建一个 hop 值 +1 的新信封副本。

        每次消息经过一个 bot 节点时，都应调用此方法让 hop 计数器递增。
        这是 TTL 保护机制的核心——当 hop 超过 ttl 时 validate() 会拒绝该消息。

        注意:
            原始信封不会被修改（不可变风格），返回的是全新实例。

        返回:
            hop 值加 1 的新 RelayEnvelope。
        """

        data = self.to_dict()
        data["hop"] = self.hop + 1
        return RelayEnvelope.from_dict(data)