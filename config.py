"""MQTT adapter configuration."""

from __future__ import annotations

from typing import ClassVar

from src.core.components.base.config import BaseConfig, Field, SectionBase, config_section


class PartnerSection(SectionBase):
    """Configured MQTT relay partner."""

    bot_id: str = Field(default="", description="伙伴 bot 的路由 ID")
    bot_name: str = Field(default="", description="伙伴 bot 的显示名称")


class MqttAdapterConfig(BaseConfig):
    """Configuration for the standalone MQTT adapter."""

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = "MQTT 适配器配置"

    @config_section("plugin", title="插件设置", tag="plugin")
    class PluginSection(SectionBase):
        """Plugin switch and config metadata."""

        enabled: bool = Field(default=True, description="启用 MQTT 适配器")
        config_version: str = Field(default="0.1.0", description="配置文件版本")

    @config_section("mqtt", title="MQTT", tag="network")
    class MqttSection(SectionBase):
        """MQTT relay identity and broker options."""

        broker_url: str = Field(default="mqtt://localhost:1883", description="MQTT broker 地址")
        bot_id: str = Field(default="", description="本 bot 的路由 ID")
        bot_name: str = Field(default="", description="本 bot 的显示名称")
        auth_token: str = Field(default="", description="可选认证 token")
        default_ttl: int = Field(default=4, description="默认中继跳数 TTL")
        default_reply_budget: int = Field(default=3, description="默认请求回复预算")
        show_system_message_logs: bool = Field(default=True, description="是否在日志中展示系统消息入站")

    @config_section("partners", title="Partners", tag="plugin")
    class PartnersSection(SectionBase):
        """Partner mapping for simple deployments."""

        bot_b: PartnerSection = Field(default_factory=PartnerSection)

    @config_section("presence", title="Presence", tag="plugin")
    class PresenceSection(SectionBase):
        """Presence and allowlist settings."""

        allowed_partner_bots: list[str] = Field(default_factory=list, description="允许通信的伙伴 bot_id 列表")
        require_known_partner: bool = Field(default=True, description="是否要求对端必须在已知伙伴列表中")

    plugin: PluginSection = Field(default_factory=PluginSection)
    mqtt: MqttSection = Field(default_factory=MqttSection)
    partners: PartnersSection = Field(default_factory=PartnersSection)
    presence: PresenceSection = Field(default_factory=PresenceSection)

    def partner_by_id(self, bot_id: str) -> PartnerSection | None:
        """Return configured partner by bot id."""

        for value in vars(self.partners).values():
            if isinstance(value, PartnerSection) and value.bot_id == bot_id:
                return value
        return None

    def first_allowed_partner(self) -> PartnerSection | None:
        """Return the first allowlisted partner config."""

        for bot_id in self.presence.allowed_partner_bots:
            partner = self.partner_by_id(bot_id)
            if partner is not None:
                return partner
        return None
