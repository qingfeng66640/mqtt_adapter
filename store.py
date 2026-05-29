"""MQTT 适配器的模块级运行时状态存储。

本模块使用模块级字典（而非数据库）存储适配器运行时的瞬时状态，
包括消息去重缓存、在线状态表、会话状态表、审计日志和事务记录。
所有状态均在进程内存中，重启后丢失——这符合适配器层的设计意图：
持久化由上层（core 层的 memory 模块）负责。

数据结构:
    - DEDUP_CACHE: 消息去重缓存（message_id → 首次到达时间）
    - PRESENCE_TABLE: 伙伴在线状态表（bot_id → PresenceRecord）
    - SESSION_TABLE: 中继会话状态表（conversation_id → RelaySession）
    - AUDIT_LOG: 安全审计日志（按时间顺序追加）
    - TRANSACTION_LOG: 事务记录日志（conversation_id → RelayTransactionRecord）
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class PresenceRecord:
    """伙伴 bot 的在线状态记录。

    每当收到 presence_update 系统消息或心跳超时时更新。
    last_seen 用于判断伙伴是否在线（超过一定时间未更新则视为离线）。
    """

    bot_id: str
    bot_name: str = ""
    status: str = "offline"
    last_seen: float = field(default_factory=time.time)
    is_known_partner: bool = False


@dataclass(slots=True)
class RelaySession:
    """中继会话的运行时状态。

    每个会话以 conversation_id 为唯一键，跟踪事务或社交对话的
    当前阶段、回复权限、配额消耗等状态。会话状态由 SessionManager
    在每次消息收发时驱动状态转移。

    关键字段:
        - state: 事务会话的当前状态（created / pending_reply / accepted / closed 等）
        - phase: 社交会话的当前阶段（opening / active / cooling / ending / closed）
        - terminal: 会话是否已终结（不再接受新消息）
        - reply_budget: 剩余回复配额，每次回复递减
        - allowed_responders: 当前阶段允许回复的 bot_id 列表
        - updated_at: 最后更新时间戳，用于会话过期清理
    """

    conversation_id: str
    peer_bot_id: str
    channel: str
    intent: str
    state: str | None = None
    terminal: bool = False
    expect_reply: bool = False
    reply_budget: int = 0
    allowed_responders: list[str] = field(default_factory=list)
    phase: str | None = None
    turn_count: int = 0
    max_turns: int = 6
    cooldown_seconds: int = 0
    cooldown_until: float = 0.0
    updated_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class RelayTransactionRecord:
    """中继事务的持久化摘要记录。

    与 RelaySession 不同，TransactionRecord 侧重于记录事务的
    关键摘要信息（主题、最终意图），用于审计和日志展示，
    而非驱动状态机。

    字段:
        - current_state: 事务当前所处的状态
        - final_intent: 事务终结时的最终意图（仅在 terminal 时设置）
        - topic / summary: 事务的主题和摘要文本
    """

    conversation_id: str
    trace_id: str
    from_bot: str
    to_bot: str
    current_state: str
    final_intent: str | None = None
    topic: str = ""
    summary: str = ""


# === 模块级状态存储 ===

DEDUP_CACHE: dict[str, float] = {}
"""消息去重缓存：message_id → 首次接收时间戳。

缓存条目有 TTL（默认 3600 秒），remember_message() 会在写入前
自动清理过期条目。
"""

PRESENCE_TABLE: dict[str, PresenceRecord] = {}
"""伙伴在线状态表：bot_id → PresenceRecord。"""

SESSION_TABLE: dict[str, RelaySession] = {}
"""会话状态表：conversation_id → RelaySession。"""

AUDIT_LOG: list[dict[str, object]] = []
"""安全审计日志，按时间顺序记录关键事件（拒绝访问、孤儿消息等）。"""

TRANSACTION_LOG: dict[str, RelayTransactionRecord] = {}
"""事务摘要日志：conversation_id → RelayTransactionRecord。"""


def reset_state() -> None:
    """清空所有模块级状态，主要用于测试环境。

    在生产环境中不应调用此函数——它会丢失所有会话状态和在线信息。
    测试用例通过调用此函数确保每个测试在干净的状态下运行。
    """

    DEDUP_CACHE.clear()
    PRESENCE_TABLE.clear()
    SESSION_TABLE.clear()
    AUDIT_LOG.clear()
    TRANSACTION_LOG.clear()


def remember_message(message_id: str, ttl_seconds: int = 3600) -> bool:
    """记录一条消息 ID 并返回是否为首次出现。

    用于 message 级别的去重——如果 MQTT QoS 导致消息重复投递，
    此函数可确保同一条消息不会被处理两次。

    实现细节:
        1. 先清理 DEDUP_CACHE 中超过 ttl_seconds 的过期条目
        2. 检查 message_id 是否已在缓存中
        3. 如果不存在则记录并返回 True，否则返回 False

    参数:
        message_id: 消息的唯一标识。
        ttl_seconds: 缓存条目的有效期（秒），默认 3600 秒。

    返回:
        True 表示消息首次出现（应处理），False 表示重复消息（应丢弃）。
    """

    now = time.time()
    expired = [key for key, seen_at in DEDUP_CACHE.items() if now - seen_at > ttl_seconds]
    for key in expired:
        DEDUP_CACHE.pop(key, None)
    if message_id in DEDUP_CACHE:
        return False
    DEDUP_CACHE[message_id] = now
    return True


def upsert_presence(record: PresenceRecord) -> None:
    """插入或更新伙伴在线状态记录。

    以 bot_id 为键，新记录直接覆盖旧记录。

    参数:
        record: 包含最新在线状态的 PresenceRecord。
    """

    PRESENCE_TABLE[record.bot_id] = record


def save_session(session: RelaySession) -> None:
    """保存或更新中继会话状态。

    自动更新 updated_at 时间戳，以 conversation_id 为键写入 SESSION_TABLE。

    参数:
        session: 要持久化的会话状态对象。
    """

    session.updated_at = time.time()
    SESSION_TABLE[session.conversation_id] = session


def get_session(conversation_id: str) -> RelaySession | None:
    """按会话 ID 获取会话状态。

    参数:
        conversation_id: 会话的唯一标识。

    返回:
        对应的 RelaySession，不存在时返回 None。
    """

    return SESSION_TABLE.get(conversation_id)


def audit(event: str, **data: object) -> None:
    """向审计日志追加一条记录。

    自动附加事件名称和时间戳，其余数据通过关键字参数传入。

    参数:
        event: 事件名称（如 "sender_not_allowed"、"orphan_transaction_continuation"）。
        **data: 事件的附加上下文数据。
    """

    AUDIT_LOG.append({"event": event, "time": time.time(), **data})


def save_transaction_record(record: RelayTransactionRecord) -> None:
    """保存或更新事务摘要记录。

    以 conversation_id 为键写入 TRANSACTION_LOG。

    参数:
        record: 要持久化的事务记录。
    """

    TRANSACTION_LOG[record.conversation_id] = record