package com.trader.defichain.mempool

import com.trader.defichain.db.DB
import com.trader.defichain.dex.*
import com.trader.defichain.http.Message
import com.trader.defichain.http.connections
import com.trader.defichain.indexer.calculateFee
import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.rpc.TX
import com.trader.defichain.util.floorPlain
import com.trader.defichain.zmq.ZMQEventType
import com.trader.defichain.zmq.newZMQEventChannel
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.encodeToJsonElement
import java.math.BigDecimal
import kotlin.coroutines.CoroutineContext

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

                if (customTX.type != "PoolSwap") continue

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
                val row = DB.PoolSwapRow(
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
                    mempool = DB.MempoolEntry(
                        blockHeight = block.height,
                        txn = txn,
                        time = time,
                    ),
                    priceImpact = 0.0,
                )

                connections.forEach {
                    try {
                        row.priceImpact = 0.0

                        val graph = it.graph
                        if (graph != null) {

                            val tokenID = getTokenId(graph.fromToken)
                            val oraclePrice = if (tokenID != null) getOraclePrice(tokenID) ?: 1.0 else 1.0
                            val graphSwap = PoolSwap(
                                tokenFrom = graph.fromToken,
                                tokenTo = graph.toToken,
                                amountFrom = 1000.0 / oraclePrice,
                                desiredResult = 1.0,
                            )

                            val pools = getActivePools()
                            val estimate = executeSwaps(listOf(graphSwap), pools, false).swapResults[0].estimate
                            val secondEstimate =
                                executeSwaps(listOf(mempoolSwap, graphSwap), pools, false).swapResults[1].estimate
                            if (estimate != secondEstimate) {
                                row.priceImpact = ((secondEstimate - estimate) / ((secondEstimate + estimate) / 2.0)) * 100.0
                            }
                        }

                        val json = Json.encodeToString(
                            Message(
                                id = "mempool-swap",
                                data = Json.encodeToJsonElement(row),
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

