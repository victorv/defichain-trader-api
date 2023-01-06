package com.trader.defichain.indexer

import com.trader.defichain.db.DBTX
import com.trader.defichain.db.insertPoolPairs
import com.trader.defichain.db.insertTX
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.getTokenSymbol
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.zmq.ZMQEvent
import com.trader.defichain.zmq.ZMQEventType
import com.trader.defichain.zmq.newZMQEventChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.coroutines.CoroutineContext

private val eventChannel = newZMQEventChannel()
private val logger = LoggerFactory.getLogger("ZMQEventQueue")
private val rawTransactions = mutableMapOf<String, ZMQRawTX>()

private var block: Block? = null

suspend fun processZMQEvents(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        val event = eventChannel.receive()
        try {
            processEvent(event)
        } catch (e: Throwable) {
            logger.error("Failed to process $event; skipping event and suspending processing for ${BLOCK_TIME}ms", e)
            delay(BLOCK_TIME)
        }
    }
}

private suspend fun processEvent(event: ZMQEvent) {

    if (event.type == ZMQEventType.HASH_BLOCK) {
        try {
            val blockHash = JsonPrimitive(event.payload)
            val newBlock = RPC.getValue<Block>(RPCMethod.GET_BLOCK, blockHash, JsonPrimitive(2))
            block = newBlock

            val poolPairs = event.poolPairs
            val oraclePrices = event.oraclePrices
            if (poolPairs != null && oraclePrices != null) {
                val dbtx = DBTX("Tokens, pool pairs and oracle prices at block height ${newBlock.height}")
                val masterNodeTX = RPC.getMasterNodeTX(newBlock.masterNode)
                val masterNode = dbtx.insertTX(
                    masterNodeTX.tx.txID, masterNodeTX.type, masterNodeTX.fee, masterNodeTX.size,
                    isConfirmed = true, valid = true
                )

                val usdtPrices = oraclePrices.map {
                    it.key to testPoolSwap(
                        PoolSwap(
                            amountFrom = 1.0,
                            tokenFrom = getTokenSymbol(it.key),
                            tokenTo = "USDT",
                            desiredResult = 1.0,
                        )
                    ).estimate
                }.toMap()
                dbtx.insertPoolPairs(newBlock, masterNode, poolPairs, usdtPrices)
                dbtx.submit()
            }

            announceZMQBlock(block!!, rawTransactions.values.toList())
        } finally {
            rawTransactions.clear()
        }
    }

    if (event.type == ZMQEventType.RAW_TX) {
        val currentBlock = block ?: return

        val hex = event.payload
        if (rawTransactions.containsKey(hex)) return

        val tx = ZMQRawTX(
            time = Instant.now().toEpochMilli(),
            blockHeight = currentBlock.height,
            txn = rawTransactions.size,
            hex = hex,
        )
        rawTransactions[hex] = tx

        if (rawTransactions.size % 100 == 0) {
            logger.info("${rawTransactions.size} raw transactions in queue")
        }
    }
}

data class ZMQRawTX(
    val time: Long,
    val blockHeight: Int,
    val hex: String,
    val txn: Int,
)