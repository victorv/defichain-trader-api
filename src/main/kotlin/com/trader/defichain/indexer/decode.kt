package com.trader.defichain.indexer

import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.rpc.TX
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigDecimal

suspend fun calculateFee(
    tx: TX,
    context: Map<String, TX>,
): BigDecimal {
    val inputs = tx.vin
        .filter { it.txID != tx.txID && it.txID != null && it.vout != null }
        .sumOf { vin ->
            (context[vin.txID] ?: decodeRawTransactions(vin.txID!!))
                .vout
                .filter { vout -> vout.n == vin.vout }
                .sumOf { vout -> BigDecimal(vout.value) }
        }

    return inputs - tx.vout.sumOf { BigDecimal(it.value) }
}

private suspend fun decodeRawTransactions(txID: String) = RPC.getValue<TX>(
    RPCMethod.GET_RAW_TRANSACTION,
    JsonPrimitive(txID),
    JsonPrimitive(true),
)