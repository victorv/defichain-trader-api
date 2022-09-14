package com.trader.defichain.config

@kotlinx.serialization.Serializable
data class RPCConfig(
    val host: String,
    val port: Int,
    val user: String,
    val password: String,
)

@kotlinx.serialization.Serializable
data class ZMQConfig(
    val host: String,
    val port: Int,
)

lateinit var zmqConfig: ZMQConfig
lateinit var rpcConfig: RPCConfig