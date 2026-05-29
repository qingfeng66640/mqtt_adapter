"""MQTT 中继控制字段的策略规则引擎。

PolicyEngine 对外发消息应用确定性的控制字段规则，确保外发信封
的 terminal、expect_reply、reply_budget 和 allowed_responders
等字段始终符合协议约定。规则是纯函数式的——不依赖外部状态，
相同输入始终产生相同输出。

主要规则:
    - transaction + notify → 终端消息，不期待回复
    - transaction + request → 非终端消息，期待回复，设置默认 reply_budget
    - 所有 terminal 消息的 expect_reply 强制为 False
"""

from __future__ import annotations

from .envelope import RelayEnvelope


class PolicyEngine:
    """对外发信封应用确定性的控制字段规则。

    策略引擎在信封通过 MQTT 发布之前执行，确保外发消息的
    协议控制字段符合约定。它不修改消息内容（payload），
    只调整元数据字段。
    """

    def apply_outbound(self, envelope: RelayEnvelope) -> RelayEnvelope:
        """对外发信封应用 terminal 和 reply_budget 策略。

        根据信封的 channel 和 intent 组合，自动设置正确的控制字段:

        - transaction + notify: 强制 terminal=True, expect_reply=False,
          reply_budget=0，因为通知类消息不应触发回复链
        - transaction + request: 强制 terminal=False, expect_reply=True,
          state="pending_reply"；如果 reply_budget ≤ 0 则默认设为 3
        - 任何 terminal=True 的信封: 强制 expect_reply=False（安全兜底）
        - 如果 allowed_responders 为空且 intent 不是 notify:
          强制 expect_reply=False（没有人可以回复）

        参数:
            envelope: 待发送的中继信封（会被原地修改）。

        返回:
            策略调整后的同一信封实例（原地修改，同时返回以方便链式调用）。
        """

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
        """判断一条中继消息是否应触发 bot 的自动回复。

        自动回复的判断条件:
        1. relay_context 存在且不为空
        2. relay_context["terminal"] 不为 True（非终端消息）
        3. relay_context["expect_reply"] 为 True（对端明确期待回复）

        参数:
            relay_context: 从消息 extra 中提取的中继上下文字典。

        返回:
            True 表示应触发自动回复逻辑，False 表示不需要回复。
        """

        if not relay_context:
            return False
        if relay_context.get("terminal") is True:
            return False
        return bool(relay_context.get("expect_reply", False))