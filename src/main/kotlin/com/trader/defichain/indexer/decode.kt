package com.trader.defichain.indexer

import com.trader.defichain.db.DB
import com.trader.defichain.rpc.*
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
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

suspend fun calculateFee(
    txID: String,
): BigDecimal {
    val tx = RPC.getValue<TX>(
        RPCMethod.GET_RAW_TRANSACTION, JsonPrimitive(txID), JsonPrimitive(true),
    )
    return calculateFee(tx, emptyMap())
}

suspend fun getMaxPrice(txID: String): Double {
    val swap = RPC.getCustomTX(txID)
    check(swap != null)
    return swap.results.getValue("maxPrice").jsonPrimitive.double
}

fun decodePoolSwap(customTX: CustomTX?): DB.PoolSwap? {
    if (customTX == null || customTX.type != "PoolSwap") return null

    val results = customTX.results
    return DB.PoolSwap(
        from = results.getValue("fromAddress").jsonPrimitive.content,
        to = results.getValue("toAddress").jsonPrimitive.content,
        tokenFrom = results.getValue("fromToken").jsonPrimitive.int,
        tokenTo = results.getValue("toToken").jsonPrimitive.int,
        amountFrom = results.getValue("fromAmount").jsonPrimitive.double,
        amountTo = null,
        maxPrice = results.getValue("maxPrice").jsonPrimitive.double,
    )
}