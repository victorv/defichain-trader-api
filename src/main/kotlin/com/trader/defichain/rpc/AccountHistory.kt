package com.trader.defichain.rpc

object AccountHistory {

    @kotlinx.serialization.Serializable
    data class PoolSwap(
        val owner: String,
        val txn: Int,
        val txid: String,
        val amounts: List<String>,
        val blockHeight: Int,
        val blockHash: String,
        val blockTime: Long,
        val type: String,
    )
}