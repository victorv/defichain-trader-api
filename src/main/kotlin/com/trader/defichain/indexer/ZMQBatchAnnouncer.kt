package com.trader.defichain.indexer

import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.rpc.TX
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

private const val retries = 5
private val zmqBlockChannel = Channel<Pair<Block, List<ZMQRawTX>>>(20, BufferOverflow.DROP_OLDEST)
private val logger = LoggerFactory.getLogger("ZMQBatchAnnouncer")

suspend fun announceZMQBlock(block: Block, rawTransactions: List<ZMQRawTX>) {
    zmqBlockChannel.send(Pair(block, rawTransactions))
}

suspend fun announceZMQBatches(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        val (block, rawTransactions) = zmqBlockChannel.receive()
        announceZMQBatch(block, rawTransactions)
    }
}

private suspend fun announceZMQBatch(block: Block, rawTransactions: List<ZMQRawTX>) {
    var success = false
    for (attempt in 1..5) {
        try {
            val zmqBatch = asZMQBatch(block, rawTransactions)
            announceZMQBatch(zmqBatch)
            success = true
            logger.info("Announced ZMQ batch at block height ${block.height}")
            break
        } catch (e: Throwable) {
            val timeout = 6000L * attempt
            logger.error("Failed to announce ZMQ batch at block height ${block.height}; retrying in `$timeout`ms", e)
            delay(timeout)
        }
    }

    if (!success) {
        logger.error("Gave up on processing announcing ZMQ batch at height ${block.height} after $retries attempts; ${rawTransactions.size} raw transaction(s) will be dropped as a result")
    }
}

private suspend fun asZMQBatch(block: Block, rawTransactions: List<ZMQRawTX>): ZMQBatch {
    val mintedByHex = block.tx.associateBy { it.hex }.toMutableMap()
    val transactionPairs = ArrayList<ZMQPair>(rawTransactions.size)
    for (rawTX in rawTransactions) {

        val mintedTX = mintedByHex[rawTX.hex]
        val tx = mintedTX ?: RPC.getValue(
            RPCMethod.DECODE_RAW_TRANSACTION,
            JsonPrimitive(rawTX.hex)
        )
        tx.hex = tx.hex ?: rawTX.hex

        mintedByHex.remove(rawTX.hex)
        transactionPairs.add(ZMQPair(rawTX, tx, mintedTX != null))
    }

    for (tx in mintedByHex.values) {
        transactionPairs.add(ZMQPair(null, tx, true))
    }

    val txContext = block.tx.associateBy { it.txID }
    return ZMQBatch(block, transactionPairs, txContext)
}

data class ZMQPair(
    val rawTX: ZMQRawTX?,
    val tx: TX,
    val isConfirmed: Boolean,
)

data class ZMQBatch(
    val block: Block,
    val tx: List<ZMQPair>,
    val txContext: Map<String, TX>,
)