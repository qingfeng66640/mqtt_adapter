"""MQTT 中继会话管理：事务状态机和社交会话控制。

SessionManager 是适配器的会话层核心，负责:

1. **事务信道 (transaction)**: 基于状态机的结构化交互
   - 状态: created → pending_reply → accepted → closed
   - 意图: notify / request / invite / accept / decline / confirm / cancel / close / reschedule / ack
   - 每次消息收发驱动状态转移，跟踪 reply_budget 和 allowed_responders

2. **社交信道 (social)**: 基于阶段的自由对话
   - 阶段: opening → active → cooling → ending → closed
   - 通过 turn_count 和 max_turns 自动推进阶段
   - 支持冷却时间 (cooldown) 控制对话节奏

3. **信封构建**: 从框架 MessageEnvelope 构建出站 RelayEnvelope，
   并将会话状态编码到 relay_context 中供下游使用。
"""

from __future__ import annotations

import time

from mofox_wire import MessageEnvelope

from . import store
from .envelope import RelayEnvelope


class SessionManager:
    """提供中继事务和社交会话的语义管理。

    事务会话使用确定性的状态机（_TRANSITIONS 字典），每个状态
    只允许特定的 intent 触发转移。社交会话使用阶段推进模型，
    基于轮次计数和冷却时间自动流转。

    使用示例::

        mgr = SessionManager()

        # 构建外发信封
        envelope = mgr.build_outbound_envelope(
            message_envelope=msg_envelope,
            from_bot="bot_a",
            from_bot_name="Bot A",
            to_bot="bot_b",
            to_bot_name="Bot B",
        )

        # 同步入站会话状态
        session = mgr.sync_inbound_transaction_session(inbound_envelope)
    """

    # 事务状态转移表:
    #   当前状态 → {intent → 下一状态}
    #   notify/request/invite 从 created 发起新会话
    #   accept/decline/close 等从 pending_reply 或 accepted 推进
    _TRANSITIONS = {
        "created": {"notify": "closed", "request": "pending_reply", "invite": "pending_reply"},
        "pending_reply": {
            "accept": "accepted",
            "decline": "closed",
            "reschedule": "reschedule_requested",
            "ack": "closed",
            "close": "closed",
            "cancel": "closed",
        },
        "accepted": {
            "confirm": "closed",
            "decline": "closed",
            "cancel": "closed",
            "reschedule": "reschedule_requested",
        },
        "reschedule_requested": {
            "confirm": "closed",
            "decline": "closed",
            "close": "closed",
            "cancel": "closed",
            "reschedule": "reschedule_requested",
        },
        "closed": {},
    }
    _SOCIAL_END_PHASES = {"ending", "closed"}
    _SOCIAL_PHASE_ORDER = ("opening", "active", "cooling", "ending", "closed")

    # ------------------------------------------------------------------
    # 出站信封构建
    # ------------------------------------------------------------------

    def build_outbound_envelope(
        self,
        *,
        message_envelope: MessageEnvelope,
        from_bot: str,
        from_bot_name: str,
        to_bot: str,
        to_bot_name: str,
        default_ttl: int = 4,
        default_reply_budget: int = 3,
    ) -> RelayEnvelope:
        """从框架 MessageEnvelope 构建外发 RelayEnvelope。

        根据 relay_context 中的 channel 类型分两路处理:

        1. **社交信道 (social)**: 委托给 build_social_envelope()
        2. **事务信道 (transaction)**: 在此方法中直接构建，流程如下:
           - 从上下文中解析 intent（显式指定 > 从会话推断 > 默认 "notify"）
           - 根据 intent 设置 terminal/expect_reply/reply_budget 等控制字段
           - 如果存在已有会话，继承其状态（reply_budget、allowed_responders 等）
           - 将新会话或更新后的会话保存到 store

        参数:
            message_envelope: 框架层的出站消息信封。
            from_bot: 发送方 bot ID。
            from_bot_name: 发送方 bot 名称。
            to_bot: 接收方 bot ID。
            to_bot_name: 接收方 bot 名称。
            default_ttl: TTL 默认值（跳数上限）。
            default_reply_budget: 回复预算默认值。

        返回:
            构建完成的 RelayEnvelope，会话状态已持久化到 store。
        """

        text = _extract_text(message_envelope)
        extra = _extract_extra(message_envelope)
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
        context = relay_context if isinstance(relay_context, dict) else {}
        channel = str(context.get("channel") or "transaction")
        if channel == "social":
            conversation_id = context.get("conversation_id")
            explicit_conversation_id = conversation_id if isinstance(conversation_id, str) and conversation_id else None
            envelope = self.build_social_envelope(
                from_bot=from_bot,
                from_bot_name=from_bot_name,
                to_bot=to_bot,
                to_bot_name=to_bot_name,
                text=text,
                conversation_id=explicit_conversation_id,
                phase=str(context.get("phase") or "opening"),
                reply_budget=_context_int(context, "reply_budget", default_reply_budget),
                cooldown_seconds=_context_int(context, "cooldown_seconds", 0),
                max_turns=_context_int(context, "max_turns", 6),
            )
            envelope.ttl = default_ttl
            trace_id = context.get("trace_id")
            if isinstance(trace_id, str) and trace_id:
                envelope.trace_id = trace_id
            self.save_social_session_from_envelope(envelope)
            return envelope

        inferred_session = self._find_session_for_outbound(
            context=context,
            message_envelope=message_envelope,
            to_bot=to_bot,
        )
        explicit_intent = context.get("intent")
        inferred_intent = self._infer_intent_from_session(inferred_session)
        intent = str(inferred_intent or explicit_intent or "notify")
        conversation_id = str(context.get("conversation_id") or (inferred_session.conversation_id if inferred_session else ""))
        expects_initial_reply = intent in {"request", "invite"}
        reply_budget = default_reply_budget if expects_initial_reply else 0
        allowed_responders = [to_bot] if expects_initial_reply else []
        terminal = intent == "notify"
        expect_reply = expects_initial_reply
        state = "pending_reply" if expects_initial_reply else "closed"
        if inferred_session is not None and inferred_intent:
            reply_budget = inferred_session.reply_budget
            allowed_responders = list(inferred_session.allowed_responders)
            state = inferred_session.state or state
            terminal = inferred_session.terminal
            expect_reply = inferred_session.expect_reply
        envelope = RelayEnvelope(
            conversation_id=conversation_id or RelayEnvelope().conversation_id,
            from_bot=from_bot,
            from_bot_name=from_bot_name,
            to_bot=to_bot,
            to_bot_name=to_bot_name,
            channel=channel if channel in {"system", "transaction", "social"} else "transaction",
            intent=intent,
            ttl=default_ttl,
            payload={"text": text, "structured": context.get("structured", {})},
            allowed_responders=allowed_responders,
            reply_budget=reply_budget,
            terminal=terminal,
            expect_reply=expect_reply,
            state=state,
        )
        if inferred_session is None:
            envelope.state = "pending_reply" if expects_initial_reply else envelope.state
            envelope.expect_reply = expects_initial_reply
            envelope.terminal = intent == "notify"
            envelope.reply_budget = default_reply_budget if expects_initial_reply else envelope.reply_budget
            envelope.allowed_responders = [to_bot] if expects_initial_reply else envelope.allowed_responders
        store.save_session(
            store.RelaySession(
                conversation_id=envelope.conversation_id,
                peer_bot_id=to_bot,
                channel=envelope.channel,
                intent=envelope.intent,
                state=envelope.state,
                terminal=envelope.terminal,
                expect_reply=envelope.expect_reply,
                reply_budget=envelope.reply_budget,
                allowed_responders=list(envelope.allowed_responders),
            )
        )
        existing_record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        if existing_record is None or envelope.intent in {"request", "invite", "notify"}:
            store.save_transaction_record(
                store.RelayTransactionRecord(
                    conversation_id=envelope.conversation_id,
                    trace_id=envelope.trace_id,
                    from_bot=from_bot,
                    to_bot=to_bot,
                    current_state=envelope.state or "",
                    final_intent=envelope.intent if envelope.terminal else None,
                    topic=text,
                    summary=text,
                )
            )
        else:
            existing_record.current_state = envelope.state or existing_record.current_state
            existing_record.final_intent = envelope.intent if envelope.terminal else existing_record.final_intent
            store.save_transaction_record(existing_record)
        return envelope

    def relay_context_from_envelope(self, envelope: RelayEnvelope) -> dict[str, object]:
        """从信封提取 relay_context 字典，嵌入到 MessageEnvelope.extra 中。

        relay_context 是传递给下游组件（chatter、action 等）的标准化上下文，
        包含会话标识、对端信息、控制字段等，让下游能根据中继状态
        做出正确决策（如是否应自动回复）。

        参数:
            envelope: 中继信封。

        返回:
            包含会话上下文信息的字典。
        """

        return {
            "conversation_id": envelope.conversation_id,
            "trace_id": envelope.trace_id,
            "channel": envelope.channel,
            "intent": envelope.intent,
            "peer_bot_id": envelope.from_bot,
            "peer_bot_name": envelope.from_bot_name,
            "state": envelope.state,
            "phase": envelope.phase,
            "terminal": envelope.terminal,
            "expect_reply": envelope.expect_reply,
            "reply_budget": envelope.reply_budget,
            "allowed_responders": list(envelope.allowed_responders),
        }

    # ------------------------------------------------------------------
    # 入站会话同步
    # ------------------------------------------------------------------

    def sync_inbound_transaction_session(self, envelope: RelayEnvelope) -> store.RelaySession | None:
        """根据入站事务信封同步本地会话状态。

        处理流程:
        1. 检查 channel 是否为 "transaction"（非事务信道返回 None）
        2. 查找已有会话，确定当前状态
        3. 根据 _TRANSITIONS 表查找状态转移目标
        4. 计算新的 terminal、reply_budget、allowed_responders 等
        5. 将更新后的会话保存到 store

        参数:
            envelope: 已验证的入站事务信封。

        返回:
            更新后的 RelaySession，如果 channel 不是 "transaction"
            或 intent 不在已知事务意图中则返回 None。
        """

        if envelope.channel != "transaction":
            return None
        if envelope.intent not in self._transaction_intents():
            return None

        existing = store.get_session(envelope.conversation_id)
        current_state = existing.state if existing is not None and existing.state else "created"
        next_state = self._TRANSITIONS.get(current_state, {}).get(envelope.intent)
        if next_state is None:
            return existing
        state = next_state
        terminal = state == "closed"
        previous_budget = existing.reply_budget if existing is not None else envelope.reply_budget
        reply_budget = 0 if terminal else max(0, int(previous_budget) - 1)
        allowed_responders = self._derive_inbound_allowed_responders(
            state=state,
            terminal=terminal,
            local_bot_id=envelope.to_bot,
        )
        expect_reply = False if terminal else bool(allowed_responders and reply_budget > 0)
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.from_bot,
            channel=envelope.channel,
            intent=envelope.intent,
            state=state,
            terminal=terminal,
            expect_reply=expect_reply,
            reply_budget=reply_budget,
            allowed_responders=allowed_responders,
            phase=envelope.phase,
        )
        store.save_session(session)
        existing_record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        store.save_transaction_record(
            store.RelayTransactionRecord(
                conversation_id=envelope.conversation_id,
                trace_id=envelope.trace_id,
                from_bot=envelope.from_bot,
                to_bot=envelope.to_bot,
                current_state=state or "",
                final_intent=envelope.intent if terminal else None,
                topic=existing_record.topic if existing_record is not None else envelope.text,
                summary=existing_record.summary if existing_record is not None else envelope.text,
            )
        )
        return session

    @staticmethod
    def _derive_inbound_allowed_responders(*, state: str, terminal: bool, local_bot_id: str) -> list[str]:
        """根据本地状态推导当前允许的回复者列表。

        规则:
        - terminal 状态下不允许任何人回复 → 返回空列表
        - pending_reply / accepted / reschedule_requested 状态下，
          仅本地 bot 可以回复（local_bot_id）
        - 其他状态 → 返回空列表

        这是从本地视角的安全推导，不依赖对端声称的 allowed_responders。

        参数:
            state: 当前会话状态。
            terminal: 是否已终结。
            local_bot_id: 本地 bot 的 ID。

        返回:
            允许回复的 bot_id 列表。
        """

        if terminal:
            return []
        if state in {"pending_reply", "accepted", "reschedule_requested"} and local_bot_id:
            return [local_bot_id]
        return []

    def sync_inbound_social_session(self, envelope: RelayEnvelope) -> store.RelaySession | None:
        """根据入站社交信封同步本地社交会话状态。

        社交会话使用阶段（phase）模型而非状态机模型。处理逻辑:
        - 使用信封中的 phase，或继承已有会话的 phase，默认 "active"
        - 根据 terminal 标志或是否处于结束阶段来判断是否终结
        - 非终结状态下保留 reply_budget 和 allowed_responders

        参数:
            envelope: 已验证的入站社交信封。

        返回:
            更新后的 RelaySession，非社交信道返回 None。
        """

        if envelope.channel != "social":
            return None
        existing = store.get_session(envelope.conversation_id)
        phase = envelope.phase or (existing.phase if existing is not None else "active")
        terminal = envelope.terminal or phase in self._SOCIAL_END_PHASES
        reply_budget = 0 if terminal else envelope.reply_budget
        allowed_responders = [] if terminal or reply_budget <= 0 else list(envelope.allowed_responders)
        expect_reply = False if terminal or reply_budget <= 0 or not allowed_responders else envelope.expect_reply
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.from_bot,
            channel="social",
            intent=envelope.intent,
            state=None,
            terminal=terminal,
            expect_reply=expect_reply,
            reply_budget=reply_budget,
            allowed_responders=allowed_responders,
            phase=phase,
            turn_count=existing.turn_count if existing is not None else 0,
            max_turns=existing.max_turns if existing is not None else 6,
            cooldown_seconds=envelope.cooldown_seconds,
            cooldown_until=existing.cooldown_until if existing is not None else 0.0,
        )
        store.save_session(session)
        return session

    # ------------------------------------------------------------------
    # 社交会话构建
    # ------------------------------------------------------------------

    def build_social_envelope(
        self,
        *,
        from_bot: str,
        from_bot_name: str,
        to_bot: str,
        to_bot_name: str,
        text: str,
        conversation_id: str | None = None,
        phase: str = "opening",
        reply_budget: int = 3,
        cooldown_seconds: int = 0,
        max_turns: int = 6,
    ) -> RelayEnvelope:
        """构建社交信道信封，附带回复控制字段。

        社交会话的生命周期:
        1. 首次消息 (opening): phase 自动推进到 "active"
        2. 已有活跃会话: 调用 advance_social_turn() 推进轮次
        3. 已结束会话: 保持结束状态，reply_budget 清零

        参数:
            from_bot: 发送方 bot ID。
            from_bot_name: 发送方 bot 名称。
            to_bot: 接收方 bot ID。
            to_bot_name: 接收方 bot 名称。
            text: 消息文本内容。
            conversation_id: 显式指定的会话 ID（可选，不指定则复用已有会话）。
            phase: 初始阶段，默认 "opening"。
            reply_budget: 回复预算。
            cooldown_seconds: 冷却时间（秒）。
            max_turns: 最大轮次。

        返回:
            构建完成的社交信道 RelayEnvelope。
        """

        existing = self._find_social_session(to_bot, conversation_id=conversation_id)
        if existing is not None and not existing.terminal:
            existing = self.advance_social_turn(
                session=existing,
                max_turns=max_turns,
                cooldown_seconds=cooldown_seconds,
            )
            phase = existing.phase or phase
            reply_budget = existing.reply_budget
            cooldown_seconds = existing.cooldown_seconds
        elif existing is not None:
            phase = existing.phase or "closed"
            reply_budget = 0
        elif phase == "opening":
            phase = "active"

        terminal = phase in ("ending", "closed") or reply_budget <= 0
        allowed_responders = [to_bot] if not terminal else []
        envelope = RelayEnvelope(
            from_bot=from_bot,
            from_bot_name=from_bot_name,
            to_bot=to_bot,
            to_bot_name=to_bot_name,
            channel="social",
            intent="say",
            payload={"text": text},
            phase=phase,
            reply_budget=reply_budget,
            cooldown_seconds=cooldown_seconds,
            allowed_responders=allowed_responders,
            terminal=terminal,
            expect_reply=not terminal,
            state=None,
        )
        if existing is not None:
            envelope.conversation_id = existing.conversation_id
        elif conversation_id is not None:
            envelope.conversation_id = conversation_id
        return self.apply_expect_reply_overrides(envelope)

    def _find_social_session(self, peer_bot_id: str, conversation_id: str | None = None) -> store.RelaySession | None:
        """查找与指定 peer bot 的社交会话。

        查找策略:
        1. 如果指定了 conversation_id，直接精确查找
        2. 否则在所有会话中匹配 peer_bot_id 和 channel == "social"
        3. 优先返回活跃的（非 terminal、非结束阶段）会话
        4. 如果有多个候选，返回最近更新的那个

        参数:
            peer_bot_id: 伙伴 bot ID。
            conversation_id: 可选的会话 ID 精确匹配。

        返回:
            匹配的 RelaySession，未找到时返回 None。
        """

        if conversation_id is not None:
            session = store.get_session(conversation_id)
            if session is not None and session.peer_bot_id == peer_bot_id and session.channel == "social":
                return session
            return None
        candidates = [
            session
            for session in store.SESSION_TABLE.values()
            if session.peer_bot_id == peer_bot_id and session.channel == "social"
        ]
        active = [session for session in candidates if not session.terminal and session.phase not in self._SOCIAL_END_PHASES]
        pool = active or candidates
        return max(pool, key=lambda session: session.updated_at) if pool else None

    def apply_expect_reply_overrides(self, envelope: RelayEnvelope) -> RelayEnvelope:
        """应用 expect_reply 覆盖规则的优先级判断。

        规则（按优先级降序）:
        1. terminal=True → expect_reply=False（终结消息不需要回复）
        2. reply_budget <= 0 → expect_reply=False（没有回复配额）
        3. allowed_responders 为空 → expect_reply=False（没人能回复）
        4. phase 处于结束阶段 → expect_reply=False（对话已结束）
        5. 以上都不满足 → expect_reply=True

        参数:
            envelope: 需要检查的信封（会被原地修改）。

        返回:
            更新后的同一信封实例。
        """

        if envelope.terminal is True:
            envelope.expect_reply = False
            return envelope
        if envelope.reply_budget <= 0:
            envelope.expect_reply = False
            return envelope
        if not envelope.allowed_responders:
            envelope.expect_reply = False
            return envelope
        if envelope.phase in self._SOCIAL_END_PHASES:
            envelope.expect_reply = False
            return envelope
        envelope.expect_reply = True
        return envelope

    def save_social_session_from_envelope(self, envelope: RelayEnvelope) -> store.RelaySession:
        """将信封中的社交会话状态持久化到 store。

        参数:
            envelope: 包含社交会话信息的信封。

        返回:
            保存后的 RelaySession 实例。
        """

        existing = store.get_session(envelope.conversation_id)
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.to_bot,
            channel="social",
            intent=envelope.intent,
            state=None,
            terminal=envelope.terminal,
            expect_reply=envelope.expect_reply,
            reply_budget=envelope.reply_budget,
            allowed_responders=list(envelope.allowed_responders),
            phase=envelope.phase,
            turn_count=existing.turn_count if existing is not None else 0,
            max_turns=existing.max_turns if existing is not None else 6,
            cooldown_seconds=envelope.cooldown_seconds,
            cooldown_until=existing.cooldown_until if existing is not None else 0.0,
        )
        store.save_session(session)
        return session

    # ------------------------------------------------------------------
    # 兼容性与辅助
    # ------------------------------------------------------------------

    def maybe_create_memory_candidate(self, *, envelope: RelayEnvelope) -> None:
        """保持 API 兼容性的占位方法。

        在原始的 bot_private_relay 中此方法负责创建记忆候选，
        但独立的 mqtt_adapter 不拥有记忆投影职责。保留此方法
        仅为了兼容可能的外部调用者。

        参数:
            envelope: 中继信封（未使用）。
        """

        _ = envelope

    # ------------------------------------------------------------------
    # 事务动作校验与执行
    # ------------------------------------------------------------------

    def validate_transaction_action(
        self,
        *,
        conversation_id: str,
        action: str,
        caller_bot: str,
        payload_complete: bool = True,
    ) -> tuple[bool, str, store.RelaySession | None]:
        """校验一个事务动作是否合法。

        检查项:
        1. 会话是否存在 → invalid_payload
        2. 会话是否已终结 → conversation_closed
        3. 当前状态是否允许此 action → state_not_allowed
        4. 调用者是否在 allowed_responders 中 → not_allowed_responder
        5. 回复预算是否耗尽 → reply_budget_exhausted
        6. payload 是否完整 → invalid_payload

        参数:
            conversation_id: 目标会话 ID。
            action: 要执行的动作（intent）。
            caller_bot: 动作发起方的 bot ID。
            payload_complete: payload 是否完整。

        返回:
            (是否合法, 状态码, 会话对象) 三元组。
            状态码: "ok" 表示合法，其他为错误原因。
        """

        session = store.get_session(conversation_id)
        if session is None:
            return False, "invalid_payload", None
        state = session.state or "created"
        if session.terminal or state == "closed":
            return False, "conversation_closed", session
        if action not in self._TRANSITIONS.get(state, {}):
            return False, "state_not_allowed", session
        if caller_bot not in session.allowed_responders:
            return False, "not_allowed_responder", session
        if session.reply_budget <= 0:
            return False, "reply_budget_exhausted", session
        if not payload_complete:
            return False, "invalid_payload", session
        return True, "ok", session

    def apply_transaction_action(self, *, conversation_id: str, action: str, caller_bot: str) -> store.RelaySession:
        """执行已验证的事务动作并推进会话状态。

        执行步骤:
        1. 查找会话（不存在则抛出异常）
        2. 根据 _TRANSITIONS 表计算新状态
        3. 更新 session 的 state、intent、reply_budget、terminal 等字段
        4. 如果是终端动作（confirm/decline/cancel/ack/close），清空 allowed_responders
        5. 如果是 accept/reschedule，将回复权限转交给对端

        参数:
            conversation_id: 目标会话 ID。
            action: 要执行的动作。
            caller_bot: 动作发起方的 bot ID（未使用，保留用于未来扩展）。

        返回:
            更新后的会话对象。

        异常:
            ValueError: 会话不存在时抛出。
        """

        session = store.get_session(conversation_id)
        if session is None:
            raise ValueError("conversation_not_found")
        current_state = session.state or "created"
        next_state = self._TRANSITIONS[current_state][action]
        terminal = next_state == "closed" or action in {"confirm", "decline", "cancel", "ack", "close"}
        session.state = next_state
        session.intent = action
        session.reply_budget = 0 if terminal else max(0, session.reply_budget - 1)
        session.terminal = terminal
        if terminal:
            session.expect_reply = False
            session.allowed_responders = []
        elif action in {"accept", "reschedule"}:
            session.expect_reply = True
            session.allowed_responders = [session.peer_bot_id]
        store.save_session(session)
        record = store.TRANSACTION_LOG.get(conversation_id)
        if record is not None:
            record.current_state = next_state
            record.final_intent = action if session.terminal else record.final_intent
            store.save_transaction_record(record)
        return session

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _find_session_for_outbound(
        self,
        *,
        context: dict[str, object],
        message_envelope: MessageEnvelope,
        to_bot: str,
    ) -> store.RelaySession | None:
        """为外发消息查找对应的已有会话。

        查找优先级:
        1. context 中的 conversation_id（精确匹配）
        2. 与 to_bot 的未关闭事务会话
        3. 与 message_info.user_info.user_id 的未关闭事务会话

        参数:
            context: relay_context 字典。
            message_envelope: 外发消息信封。
            to_bot: 目标 bot ID。

        返回:
            匹配的会话，没有则返回 None。
        """

        conversation_id = context.get("conversation_id")
        if isinstance(conversation_id, str) and conversation_id:
            return store.get_session(conversation_id)
        for session in store.SESSION_TABLE.values():
            if session.peer_bot_id == to_bot and session.channel == "transaction" and (session.state or "") not in {"closed"}:
                return session
        user_info = (message_envelope.get("message_info") or {}).get("user_info", {})
        user_id = user_info.get("user_id") if isinstance(user_info, dict) else None
        if isinstance(user_id, str):
            for session in store.SESSION_TABLE.values():
                if session.peer_bot_id == user_id and session.channel == "transaction" and (session.state or "") not in {"closed"}:
                    return session
        return None

    @staticmethod
    def _infer_intent_from_session(session: store.RelaySession | None) -> str | None:
        """从当前事务会话状态推断外发 intent。

        推断规则:
        - accepted 状态 → intent="accept"（确认接受）
        - reschedule_requested 状态 → intent="reschedule"
        - closed 状态 → 如果之前是 notify/confirm/decline 等则保持，
          否则默认 "close"

        参数:
            session: 当前会话状态。

        返回:
            推断出的 intent 字符串，无会话时返回 None。
        """

        if session is None:
            return None
        state = session.state or ""
        if state == "accepted":
            return "accept"
        if state == "reschedule_requested":
            return "reschedule"
        if state == "closed":
            if session.intent in {"notify", "confirm", "decline", "cancel", "ack", "close"}:
                return session.intent
            return "close"
        return None

    @classmethod
    def _transaction_intents(cls) -> set[str]:
        """返回所有已知的事务 intent 集合。

        从 _TRANSITIONS 表中提取所有可能的 intent 值，
        包括 created 状态的发起 intent 和所有转移表中的 action intent。

        返回:
            所有已知 intent 字符串的集合。
        """

        intents = set(cls._TRANSITIONS["created"])
        for transitions in cls._TRANSITIONS.values():
            intents.update(transitions)
        return intents

    # ------------------------------------------------------------------
    # 社交会话阶段推进
    # ------------------------------------------------------------------

    @staticmethod
    def _next_social_phase(current: str) -> str:
        """返回社交阶段链中的下一个阶段。

        阶段链: opening → active → cooling → ending → closed
        如果当前阶段不在链中或已是最后一个阶段，返回 "closed"。

        参数:
            current: 当前阶段名称。

        返回:
            下一个阶段名称。
        """

        try:
            idx = SessionManager._SOCIAL_PHASE_ORDER.index(current)
            if idx + 1 < len(SessionManager._SOCIAL_PHASE_ORDER):
                return SessionManager._SOCIAL_PHASE_ORDER[idx + 1]
        except ValueError:
            pass
        return "closed"

    def advance_social_turn(
        self,
        *,
        session: store.RelaySession,
        max_turns: int = 6,
        cooldown_seconds: int = 0,
    ) -> store.RelaySession:
        """推进社交会话的轮次计数和阶段。

        阶段推进阈值:
        - opening + 1 轮 → active
        - active 达到 max_turns 的 70% → cooling
        - cooling 达到 max_turns → ending

        当 reply_budget 耗尽时也会推进到 ending。
        进入 cooling 阶段时设置冷却时间（cooldown_until）。

        参数:
            session: 当前社交会话。
            max_turns: 最大轮次（用于计算阶段阈值）。
            cooldown_seconds: 冷却时间（秒）。

        返回:
            更新后的会话对象。
        """

        session.turn_count += 1
        session.max_turns = max_turns
        session.cooldown_seconds = cooldown_seconds
        phase = session.phase or "opening"
        turns = session.turn_count
        if phase == "opening" and turns >= 1:
            phase = "active"
        if phase == "active" and turns >= int(max_turns * 0.7):
            phase = "cooling"
        if phase == "cooling" and turns >= max_turns:
            phase = "ending"
        session.reply_budget = max(0, session.reply_budget - 1)
        if session.reply_budget <= 0:
            phase = "ending" if phase != "closed" else phase
        if phase in ("ending", "closed"):
            session.terminal = True
            session.expect_reply = False
            session.reply_budget = 0
            session.allowed_responders = []
        else:
            session.terminal = False
            session.expect_reply = bool(session.allowed_responders)
        if cooldown_seconds > 0 and phase == "cooling":
            session.cooldown_until = time.time() + cooldown_seconds
        session.phase = phase
        store.save_session(session)
        return session

    def is_social_in_cooldown(self, session: store.RelaySession) -> bool:
        """检查会话是否处于冷却窗口中。

        冷却期间应暂停自动回复，给对话双方留出缓冲时间。

        参数:
            session: 要检查的会话。

        返回:
            True 表示当前时间仍在冷却窗口内。
        """

        if session.channel != "social":
            return False
        return session.cooldown_until > time.time()

    def force_social_ending(self, session: store.RelaySession) -> store.RelaySession:
        """强制将社交会话推进到 ending 阶段。

        用于外部触发对话结束（如超时、管理员干预等场景）。
        调用后 session 将标记为 terminal，不再接受新消息。

        参数:
            session: 要强制结束的会话。

        返回:
            更新后的会话对象。
        """

        session.phase = "ending"
        session.terminal = True
        session.expect_reply = False
        session.reply_budget = 0
        store.save_session(session)
        return session


# ------------------------------------------------------------------
# 模块级辅助函数
# ------------------------------------------------------------------


def _extract_text(message_envelope: MessageEnvelope) -> str:
    """从 MessageEnvelope 中提取拼接后的文本内容。

    遍历 message_segment 列表，提取所有 type="text" 的 segment，
    将其 data 字段拼接为单个字符串返回。

    参数:
        message_envelope: 框架层消息信封。

    返回:
        所有文本段的拼接结果，无文本时返回空字符串。
    """

    segments = message_envelope.get("message_segment") or []
    if isinstance(segments, dict):
        segments = [segments]
    text_parts: list[str] = []
    for segment in segments:
        if isinstance(segment, dict) and segment.get("type") == "text":
            text_parts.append(str(segment.get("data", "")))
    return "".join(text_parts)


def _extract_extra(message_envelope: MessageEnvelope) -> dict[str, object]:
    """从 MessageEnvelope 中安全提取 message_info.extra 字典。

    处理各种边界情况：message_info 为 None、extra 不是 dict 等，
    始终返回一个字典（可能为空）。

    参数:
        message_envelope: 框架层消息信封。

    返回:
        extra 字典，不存在或类型不对时返回空字典。
    """

    message_info = message_envelope.get("message_info") or {}
    extra = message_info.get("extra") if isinstance(message_info, dict) else None
    return extra if isinstance(extra, dict) else {}


def _context_int(context: dict[str, object], key: str, default: int) -> int:
    """从 relay_context 中安全提取非负整数值。

    处理类型转换和边界情况：值不存在、不是数字、为负数等，
    在这些情况下返回默认值。

    参数:
        context: relay_context 字典。
        key: 要提取的键。
        default: 默认值（当值无效时使用）。

    返回:
        提取的非负整数值，或默认值。
    """

    value = context.get(key)
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default