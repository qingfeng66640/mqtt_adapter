"""系统信道消息的短路处理。

系统信道（channel="system"）承载的是 bot 间的控制面消息，包括
在线状态更新、取消、关闭、错误确认等。这些消息不需要进入 LLM 推理
管道，应由 SystemChannelHandler 在适配器层直接消费。
"""

from __future__ import annotations

from . import store
from .envelope import RelayEnvelope
from .presence import PresenceManager


class SystemChannelHandler:
    """处理中继系统信道消息，跳过 LLM 推理管道。

    系统消息是 bot 间的元通信——presence_update 更新在线状态，
    cancel/close/error/ack/heartbeat/typing 记录审计日志。
    这些消息绝不应进入 chatter 的 LLM 推理流程。
    """

    def __init__(self, presence_manager: PresenceManager) -> None:
        """初始化系统信道处理器。

        参数:
            presence_manager: 在线状态管理器实例，用于处理 presence_update 消息。
        """

        self.presence_manager = presence_manager

    def handle(self, envelope: RelayEnvelope) -> bool:
        """处理一条系统信道消息并返回是否已消费。

        根据 intent 类型分类处理:
        - presence_update: 调用 PresenceManager.update_from_envelope()
          更新对应 bot 的在线状态
        - cancel / close / error / ack / heartbeat / typing:
          记录审计日志，不做其他动作

        注意:
            此方法仅处理 channel == "system" 的消息。对于非系统信道
            消息，直接返回 False 表示未消费，让调用方继续走正常管道。

        参数:
            envelope: 待处理的中继信封。

        返回:
            True 表示消息已被消费（channel 为 system），
            False 表示消息不是系统信道消息，需要上层继续处理。
        """

        if envelope.channel != "system":
            return False
        if envelope.intent == "presence_update":
            self.presence_manager.update_from_envelope(envelope)
        elif envelope.intent in {"cancel", "close", "error", "ack", "heartbeat", "typing"}:
            store.audit("system_event", intent=envelope.intent, from_bot=envelope.from_bot)
        return True