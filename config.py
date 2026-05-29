"""MQTT 适配器配置模块。

定义 MQTT 适配器的完整配置结构，包括插件开关、MQTT 连接参数、
伙伴 bot 映射和在线状态管理。配置类遵循 Neo-MoFox 框架的 BaseConfig /
SectionBase 体系，支持从 TOML 文件自动加载和热重载。

使用示例::

    config = MqttAdapterConfig.load("config/plugins/mqtt_adapter/config.toml")
    partner = config.partner_by_id("114514")  # 按 bot_id 查找伙伴
"""

from __future__ import annotations

from typing import ClassVar

from src.core.components.base.config import BaseConfig, Field, SectionBase, config_section


class PartnerSection(SectionBase):
    """已配置的 MQTT 中继伙伴 bot。

    每个 PartnerSection 代表一个可通信的对端 bot，包含其路由 ID 和显示名称。
    在简单的一对一部署中只需配置一个伙伴；多伙伴场景下可以在配置文件中
    按需增加更多 section 字段。
    """

    bot_id: str = Field(default="", description="伙伴 bot 的路由 ID，用于 MQTT topic 寻址和会话匹配")
    bot_name: str = Field(default="", description="伙伴 bot 的显示名称，用于日志和用户界面展示")


class MqttAdapterConfig(BaseConfig):
    """MQTT 适配器的独立配置文件。

    配置文件按 TOML section 组织为四个功能区域：
    - [plugin]：插件总开关和版本元数据
    - [mqtt]：Broker 连接、本机身份和中继参数
    - [partners]：对端 bot 的静态映射
    - [presence]：在线状态白名单和安全策略

    配置文件的默认路径由框架约定推断，通常位于
    ``config/plugins/mqtt_adapter/config.toml``。
    """

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = "MQTT 适配器配置"

    @config_section("plugin", title="插件设置", tag="plugin")
    class PluginSection(SectionBase):
        """插件级别的开关和元数据，控制适配器的启用/禁用。"""

        enabled: bool = Field(default=True, description="启用 MQTT 适配器。设为 false 时适配器不会启动 MQTT 连接")
        config_version: str = Field(default="0.1.0", description="配置文件版本号，用于兼容性检查")

    @config_section("mqtt", title="MQTT", tag="network")
    class MqttSection(SectionBase):
        """MQTT Broker 连接参数和本地 bot 身份配置。

        所有字段均在适配器启动时读取，修改后需要重启插件才能生效。
        """

        broker_url: str = Field(default="mqtt://localhost:1883", description="MQTT broker 地址，支持 mqtt:// 和 mqtts:// 协议")
        bot_id: str = Field(default="", description="本 bot 的唯一路由 ID，必须为非空字符串。用于构造 MQTT client_id 和 topic")
        bot_name: str = Field(default="", description="本 bot 的显示名称，用于中继协议中的 from_bot_name 字段")
        auth_token: str = Field(default="", description="可选的 MQTT 认证 token。不为空时将用作 MQTT 连接的密码")
        default_ttl: int = Field(default=4, description="默认中继跳数上限（TTL），防止消息在 bot 间无限循环转发")
        default_reply_budget: int = Field(default=3, description="默认请求-回复预算，控制一个事务会话中允许的最大回复轮次")
        show_system_message_logs: bool = Field(default=True, description="是否在日志中打印系统信道消息（如 presence_update）的入站记录")

    @config_section("partners", title="Partners", tag="plugin")
    class PartnersSection(SectionBase):
        """伙伴 bot 的静态配置映射。

        在简单的一对一或一对多部署中，可以直接在此处列出所有可通信的对端 bot。
        每个字段名可以自行命名（如 bot_b、bot_c），PartnerSection 中的 bot_id
        才是参与路由匹配的关键字段。
        """

        bot_b: PartnerSection = Field(default_factory=PartnerSection)

    @config_section("presence", title="Presence", tag="plugin")
    class PresenceSection(SectionBase):
        """在线状态和安全策略配置。

        控制哪些对端 bot 可以与本 bot 通信，以及是否强制要求对端在已知列表中。
        """

        allowed_partner_bots: list[str] = Field(default_factory=list, description="允许通信的伙伴 bot_id 列表。不在列表中的 bot 发来的非系统消息将被拒绝")
        require_known_partner: bool = Field(default=True, description="是否要求对端必须在 allowed_partner_bots 中。设为 false 将允许所有 bot 通信（不推荐）")

    plugin: PluginSection = Field(default_factory=PluginSection)
    mqtt: MqttSection = Field(default_factory=MqttSection)
    partners: PartnersSection = Field(default_factory=PartnersSection)
    presence: PresenceSection = Field(default_factory=PresenceSection)

    def partner_by_id(self, bot_id: str) -> PartnerSection | None:
        """根据 bot_id 查找已配置的伙伴。

        遍历 PartnersSection 中的所有字段，返回第一个 bot_id 匹配的 PartnerSection。
        匹配仅基于 bot_id 字段，不关心字段名（如 bot_b、bot_c）。

        参数:
            bot_id: 要查找的伙伴 bot 的路由 ID。

        返回:
            匹配的 PartnerSection，未找到时返回 None。
        """

        for value in vars(self.partners).values():
            if isinstance(value, PartnerSection) and value.bot_id == bot_id:
                return value
        return None

    def first_allowed_partner(self) -> PartnerSection | None:
        """返回白名单中第一个有效的伙伴配置。

        遍历 presence.allowed_partner_bots 列表，按顺序查找第一个在 partners
        中有对应配置项且 bot_id 匹配的伙伴。用于当消息中未明确指定目标伙伴时
        的回退路由。

        返回:
            第一个有效伙伴的 PartnerSection，没有可用伙伴时返回 None。
        """

        for bot_id in self.presence.allowed_partner_bots:
            partner = self.partner_by_id(bot_id)
            if partner is not None:
                return partner
        return None