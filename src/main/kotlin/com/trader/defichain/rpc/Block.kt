package com.trader.defichain.rpc

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonNames

@Serializable
data class TX(
    @JsonNames("txid")
    val txID: String,
    val vin: List<Vin>,
    val vout: List<Vout>,
    var hex: String? = null,
) {
    var txn = -1
}

@Serializable
data class Vin(
    @JsonNames("txid")
    val txID: String? = null,
    val vout: Int? = null
)

@Serializable
data class Vout(
    val n: Int,
    val value: Double,
)