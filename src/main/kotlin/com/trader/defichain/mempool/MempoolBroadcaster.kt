package com.trader.defichain.mempool

import com.trader.defichain.db.DB
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.getTokenSymbol
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.indexer.calculateFee
import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.rpc.TX
import com.trader.defichain.util.floorPlain
import com.trader.defichain.zmq.ZMQEventType
import com.trader.defichain.zmq.newZMQEventChannel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigDecimal
import java.util.*
import kotlin.coroutines.CoroutineContext

val connections: MutableSet<Connection> = Collections.synchronizedSet(LinkedHashSet())

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
                    it.channel.send("""{"height":${block.height}}""")
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

                val amountTo = testPoolSwap(
                    PoolSwap(
                        tokenFrom = getTokenSymbol(swap.fromToken.toString()),
                        tokenTo = getTokenSymbol(swap.toToken.toString()),
                        amountFrom = swap.fromAmount,
                        desiredResult = 1.0,
                    )
                ).estimate

                val row = DB.PoolSwapRow(
                    txID = rawTX.txID,
                    fee = fee.floorPlain(),
                    tokenFrom = getTokenSymbol(swap.fromToken.toString()),
                    tokenTo = getTokenSymbol(swap.toToken.toString()),
                    amountFrom = BigDecimal(swap.fromAmount).floorPlain(),
                    amountTo = BigDecimal(amountTo).floorPlain(),
                    from = swap.fromAddress,
                    to = swap.toAddress,
                    maxPrice = BigDecimal(swap.maxPrice).floorPlain(),
                    mempool = DB.MempoolEntry(
                        blockHeight = block.height,
                        txn = txn,
                        time = time,
                    ),
                    block = null,
                )

                val json = Json.encodeToString(row)

                connections.forEach {
                    try {
                        it.channel.send(json)
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

class Connection {

    val channel = Channel<String>(10, BufferOverflow.DROP_OLDEST)

    fun close() {
        channel.close()
    }
}