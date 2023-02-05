package com.trader.defichain.mempool

import com.trader.defichain.db.search.MempoolEntry
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.dex.*
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
import kotlin.math.roundToInt

suspend fun sendMempoolEvents(coroutineContext: CoroutineContext) {
    val channel = newZMQEventChannel()
    var block: Block? = null
    var txn = 0
    val inMempool = mutableSetOf<String>()

    while (coroutineContext.isActive) {
        val event = channel.receive()
        if (event.type == ZMQEventType.HASH_BLOCK) {
            try {
                inMempool.clear()
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
            if (inMempool.contains(event.payload)) {
                continue
            }
            if (inMempool.size < 2500) {
                inMempool.add(event.payload)
            }

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
    var modifiedFee = BigDecimal(fee)
    val details = when {
        customTX.isAuctionBid() -> {
            val v = customTX.asAuctionBid()
            val amount = v.amount()
            val amounts = listOf(amount.first to amount.second)
            amountsAsString(amounts)
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
            val amounts = listOf(abs(v.amountA) to v.tokenA, abs(v.amountB) to v.tokenB)
            amountsAsString(amounts)
        }
        customTX.isRemovePoolLiquidity() -> {
            val v = customTX.asRemovePoolLiquidity()
            "${abs(v.shares)} ${getTokenSymbol(v.poolID)}" to 0.0
        }
        customTX.isSetOracleData() -> {
            val v = customTX.asSetOracleData()
            val prices = if (v == 1) "price" else "prices"
            "sets $v token $prices" to 0.0
        }
        customTX.isAnyAccountsToAccounts() -> {
            val v = customTX.asAnyAccountsToAccounts()
            amountsAsString(v)
        }
        customTX.isAccountToAccount() -> {
            val v = customTX.asAccountToAccount()
            amountsAsString(v)
        }
        customTX.isAccountToUtxos() -> {
            val v = customTX.asAccountToUtxos()
            v.forEach { modifiedFee += BigDecimal(it.first) }
            amountsAsString(customTX.asAccountToUtxos())
        }
        customTX.isUtxosToAccount() -> {
            amountsAsString(customTX.asUtxosToAccount())
        }
        else -> "" to 0.0
    }

    return Json.encodeToJsonElement(
        ShortDescription(
            type = customTX.type,
            blockHeight = block.height,
            txn = txn,
            time = time,
            fee = modifiedFee.floorPlain(),
            description = details.first,
            usdtAmount = details.second,
            details = null
        )
    )
}

private fun amountsAsString(amounts: List<Pair<Double, Int>>): Pair<String, Double> {
    val usdtAmountsByToken = amounts
        .filter { !hasPool(it.second) }
        .map {
            if (hasPool(it.second)) {
                getTokenSymbol(it.second) to 0.0
            } else {
                val (swap, usdt) = toUSDT(it)
                swap.tokenFrom to usdt
            }
        }
        .groupBy { it.first }
        .map { it.key to it.value.sumOf { swap -> swap.second } }
    val usdtSum = usdtAmountsByToken.sumOf { it.second }
    return usdtAmountsByToken.joinToString(", ") {
        "$${it.second} ${it.first}"
    } to (usdtSum * 100.0).roundToInt() / 100.0
}

private fun toUSDT(it: Pair<Double, Int>): Pair<PoolSwap, Double> {
    val swap = PoolSwap(
        tokenFrom = getTokenSymbol(it.second),
        amountFrom = abs(it.first),
        tokenTo = "USDT",
        desiredResult = 1.0,
    )
    val usdt = (testPoolSwap(swap).estimate * 100.0).roundToInt() / 100.0
    return Pair(swap, usdt)
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
        blockHeight = block.height,
    )

    val (_, usdtFrom) = toUSDT(swap.fromAmount to getTokenId(row.tokenFrom)!!)
    val (_, usdtTo) = if (amountTo == 0.0) null to 0.0 else toUSDT(amountTo to getTokenId(row.tokenTo)!!)
    return Json.encodeToJsonElement(
        ShortDescription(
            type = customTX.type,
            blockHeight = block.height,
            txn = txn,
            time = time,
            fee = row.fee,
            description = "$$usdtFrom ${row.tokenFrom} to $$usdtTo ${row.tokenTo}",
            details = Json.encodeToJsonElement(row),
            usdtAmount = usdtFrom,
        )
    )
}

@kotlinx.serialization.Serializable
data class ShortDescription(
    val type: String,
    val fee: String,
    val description: String,
    val blockHeight: Int,
    val usdtAmount: Double,
    val txn: Int,
    val time: Long,
    val details: JsonElement?
)
