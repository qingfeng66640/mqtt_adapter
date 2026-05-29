"""MQTT 适配器插件包。

本插件将 Neo-MoFox 的 bot 间通信能力通过 MQTT 协议暴露为独立的适配器组件。
它不依赖 bot_private_relay，可单独部署，让任意两个 Bot 实例通过 MQTT Broker
进行中继通信，支持 transaction（事务）、social（社交）和 system（系统）三种信道。
"""