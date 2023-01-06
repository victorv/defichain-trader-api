package com.trader.defichain.mempool

import com.trader.defichain.db.search.MempoolEntry
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.getOraclePrice
import com.trader.defichain.dex.getTokenSymbol
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.http.Message
import com.trader.defichain.http.connections
import com.trader.defichain.indexer.calculateFee
import com.trader.defichain.rpc.*
import com.trader.defichain.util.floorPlain
import com.trader.defichain.zmq.ZMQEventType
import com.trader.defichain.zmq.newZMQEventChannel
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.encodeToJsonElement
import java.math.BigDecimal
import kotlin.coroutines.CoroutineContext
import kotlin.math.abs

suspend fun sendMempoolEvents(coroutineContext: CoroutineContext) {
    val channel = newZMQEventChannel()
    var block: Block? = null
    var txn = 0

    while (coroutineContext.isActive) {
        val event = channel.receive()
        if (event.type == ZMQEventType.HASH_BLOCK) {
            try {
                block = RPC.getValue<Block>(RPCMethod.GET_BLOCK, JsonPrimitive(event.payload), JsonPrimitive(2))
            } catch (e: Throwable) {
                e.printStackTrace()
                continue
            }

            connections.forEach {
                try {
                    val message = Message(
                        id = "block",
                        data = JsonPrimitive(block.height)
                    )
                    it.send(Json.encodeToString(message))
                } catch (e: Throwable) {
                    it.close()
                }
            }
        } else if (event.type == ZMQEventType.RAW_TX && block != null) {
            val time = System.currentTimeMillis()

            try {
                val customTX = RPC.decodeCustomTX(event.payload) ?: continue
                val rawTX = RPC.getValue<TX>(
                    RPCMethod.DECODE_RAW_TRANSACTION,
                    JsonPrimitive(event.payload),
                )
                val fee = calculateFee(rawTX, mapOf())

                val json = if (customTX.type == "PoolSwap")
                    asSwap(customTX, rawTX, fee, block, txn, time)
                else asShortDescription(customTX, fee.floorPlain(), block, txn, time)

                connections.forEach {
                    try {
                        val json = Json.encodeToString(
                            Message(
                                id = "mempool-swap",
                                data = json,
                            )
                        )

                        it.send(json)
                    } catch (e: Throwable) {
                        it.close()
                    }
                }

                txn++
            } catch (e: Throwable) {
                e.printStackTrace()
                continue
            }
        }
    }
}

fun asShortDescription(customTX: CustomTX.Record, fee: String, block: Block, txn: Int, time: Long): JsonElement {
    val description = when {
        customTX.isAuctionBid() -> {
            val v = customTX.asAuctionBid()
            val amount = v.amount()
            val tokenSymbol = getTokenSymbol(amount.second)
            "${amount.first} $tokenSymbol"
        }
        customTX.isTakeLoan() -> {
            val v = customTX.asTakeLoan()
            amountsAsString(v.amounts)
        }
        customTX.isPaybackLoan() -> {
            val v = customTX.asPaybackLoan()
            amountsAsString(v.amounts)
        }
        customTX.isWithdrawFromVault() -> {
            val v = customTX.asWithdrawFromVault()
            amountsAsString(v.amounts)
        }
        customTX.isDepositToVault() -> {
            val v = customTX.asDepositToVault()
            amountsAsString(v.amounts)
        }
        customTX.isAddPoolLiquidity() -> {
            val v = customTX.asAddPoolLiquidity()
            "${abs(v.amountA)} ${getTokenSymbol(v.tokenA)} and ${abs(v.amountB)} ${getTokenSymbol(v.tokenB)}"
        }
        customTX.isRemovePoolLiquidity() -> {
            val v = customTX.asRemovePoolLiquidity()
            "${abs(v.shares)} ${getTokenSymbol(v.poolID)}"
        }
        else -> ""
    }

    return Json.encodeToJsonElement(
        ShortDescription(
            type = customTX.type,
            blockHeight = block.height,
            txn = txn,
            time = time,
            fee = fee,
            description = description,
            details = null
        )
    )
}

private fun amountsAsString(amounts: List<Pair<Double, Int>>): String {
    return amounts.joinToString(", ") {
        "${abs(it.first)} ${getTokenSymbol(it.second)}"
    }
}

private fun asSwap(
    customTX: CustomTX.Record,
    rawTX: TX,
    fee: BigDecimal,
    block: Block,
    txn: Int,
    time: Long
): JsonElement {
    val swap = customTX.asPoolSwap()
    val mempoolSwap = PoolSwap(
        tokenFrom = getTokenSymbol(swap.fromToken),
        tokenTo = getTokenSymbol(swap.toToken),
        amountFrom = swap.fromAmount,
        desiredResult = 1.0,
    )

    val amountTo = testPoolSwap(mempoolSwap).estimate

    val fromOraclePrice = getOraclePrice(swap.fromToken)
    val fromAmountUSD = (fromOraclePrice ?: 0.0) * swap.fromAmount
    val toOraclePrice = getOraclePrice(swap.toToken)
    val toAmountUSD = (toOraclePrice ?: 0.0) * amountTo

    val tokenTo = getTokenSymbol(swap.toToken)
    val row = PoolSwapRow(
        fromAmountUSD = fromAmountUSD,
        toAmountUSD = toAmountUSD,
        txID = rawTX.txID,
        fee = fee.floorPlain(),
        amountFrom = BigDecimal(swap.fromAmount).floorPlain(),
        amountTo = BigDecimal(amountTo).floorPlain(),
        tokenFrom = getTokenSymbol(swap.fromToken),
        tokenTo = tokenTo,
        tokenToAlt = tokenTo,
        maxPrice = BigDecimal(swap.maxPrice).floorPlain(),
        from = swap.fromAddress,
        to = swap.toAddress,
        block = null,
        id = -1,
        mempool = MempoolEntry(
            blockHeight = block.height,
            txn = txn,
            time = time,
        ),
        priceImpact = 0.0,
    )
    return Json.encodeToJsonElement(
        ShortDescription(
            type = customTX.type,
            blockHeight = block.height,
            txn = txn,
            time = time,
            fee = row.fee,
            description = "${row.amountFrom} ${row.tokenFrom} to ${row.amountTo} ${row.tokenTo}",
            details = Json.encodeToJsonElement(row),
        )
    )
}

@kotlinx.serialization.Serializable
data class ShortDescription(
    val type: String,
    val fee: String,
    val description: String,
    val blockHeight: Int,
    val txn: Int,
    val time: Long,
    val details: JsonElement?
)
