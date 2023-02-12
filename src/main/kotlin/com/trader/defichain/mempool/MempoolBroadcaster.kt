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
import kotlinx.serialization.json.*
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
                else describe(customTX, fee.floorPlain(), block, txn, time)

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

fun describe(customTX: CustomTX.Record, fee: String, block: Block, txn: Int, time: Long): JsonElement {
    var modifiedFee = BigDecimal(fee)

    val owner = when {
        customTX.results.containsKey("owner") -> customTX.results["owner"]?.jsonPrimitive?.content
        customTX.results.containsKey("from") -> {
            val owner = customTX.results["from"]
            if (owner is JsonObject) {
                val addresses = owner.entries.map { it.key }
                if (addresses.size == 1) addresses.first() else "${addresses.size} addresses"
            }
            else owner?.jsonPrimitive?.content
        }
        customTX.results.containsKey("shareaddress") -> customTX.results["shareaddress"]?.jsonPrimitive?.content
        customTX.results.containsKey("to") -> customTX.results["to"]?.jsonPrimitive?.content
        else -> null
    } ?: ""

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
            """<span class="amount">${abs(v.shares)}</span> <span class="token">${getTokenSymbol(v.poolID)}</span>""" to 0.0
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
        MempoolTXSummary(
            type = customTX.type,
            blockHeight = block.height,
            txn = txn,
            time = time,
            fee = modifiedFee.floorPlain(),
            description = details.first,
            usdtAmount = details.second,
            details = null,
            owner = owner
        )
    )
}

private fun amountsAsString(amounts: List<Pair<Double, Int>>): Pair<String, Double> {
    val validAmounts = amounts.filter { !hasPool(it.second) }
    val usdSum = validAmounts.sumOf {
        toUSDT(it)
    }

    val amountSummedByToken = validAmounts
        .map {
            getTokenSymbol(it.second) to abs(it.first)
        }
        .groupBy { it.first }
        .map { it.key to BigDecimal(it.value.sumOf { swap -> swap.second }).floorPlain() }

    return amountSummedByToken.joinToString(", ") {
        """<span class="amount">${it.second}</span> <span class="token">${it.first}</span>"""
    } to (usdSum * 100.0).roundToInt() / 100.0
}

private fun toUSDT(it: Pair<Double, Int>): Double {
    val swap = PoolSwap(
        tokenFrom = getTokenSymbol(it.second),
        amountFrom = abs(it.first),
        tokenTo = "USDT",
        desiredResult = 1.0,
    )
    return (testPoolSwap(swap).estimate * 100.0).roundToInt() / 100.0
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

    val amountTo = BigDecimal(testPoolSwap(mempoolSwap).estimate).floorPlain()

    val fromOraclePrice = getOraclePrice(swap.fromToken)
    val fromAmountUSD = (fromOraclePrice ?: 0.0) * swap.fromAmount

    val tokenTo = getTokenSymbol(swap.toToken)
    val row = PoolSwapRow(
        txID = rawTX.txID,
        fee = fee.floorPlain(),
        amountFrom = BigDecimal(swap.fromAmount).floorPlain(),
        amountTo = amountTo,
        tokenFrom = getTokenSymbol(swap.fromToken),
        tokenTo = tokenTo,
        maxPrice = BigDecimal(swap.maxPrice).floorPlain(),
        from = swap.fromAddress,
        to = swap.toAddress,
        block = null,
        mempool = MempoolEntry(
            blockHeight = block.height,
            txn = txn,
            time = time,
        ),
        tokenToAlt = tokenTo,
        id = -1,
        priceImpact = 0.0,
        fromAmountUSD = fromAmountUSD,
        toAmountUSD = 0.0,
        blockHeight = block.height,
        dusd = "0.0",
        inverseDUSD = "0.0",
    )

    val fromDesc = """<span class="amount">${row.amountFrom}</span> <span class="token">${row.tokenFrom}</span>"""
    val toDesc = """<span class="amount">$amountTo</span> <span class="token">${row.tokenTo}</span>"""

    val usdValue = toUSDT(swap.fromAmount to getTokenId(row.tokenFrom)!!)
    return Json.encodeToJsonElement(
        MempoolTXSummary(
            type = customTX.type,
            blockHeight = block.height,
            txn = txn,
            time = time,
            fee = row.fee,
            description = "$fromDesc to $toDesc",
            details = Json.encodeToJsonElement(row),
            usdtAmount = usdValue,
            owner = row.from
        )
    )
}

@kotlinx.serialization.Serializable
data class MempoolTXSummary(
    val type: String,
    val fee: String,
    val description: String,
    val blockHeight: Int,
    val usdtAmount: Double,
    val txn: Int,
    val time: Long,
    val owner: String,
    val details: JsonElement?
)
