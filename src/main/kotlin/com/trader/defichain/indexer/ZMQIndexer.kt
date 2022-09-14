package com.trader.defichain.indexer

import com.trader.defichain.db.DB
import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.rpc.TX
import com.trader.defichain.zmq.ZMQEvent
import com.trader.defichain.zmq.ZMQEventType
import com.trader.defichain.zmq.newZMQEventChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import kotlin.coroutines.CoroutineContext

private const val retries = 5
private val eventChannel = newZMQEventChannel()
private val logger = LoggerFactory.getLogger("ZMQIndexer")
private val dbUpdater = DB.createUpdater()
private val rawTransactions = mutableMapOf<String, RawZMQTransaction>()
private var block: Block? = null

suspend fun processZMQEvents(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        var event: ZMQEvent? = null
        try {
            event = eventChannel.receive()
            processEvent(event)
        } catch (e: Throwable) {
            logger.error("Failed to process $event; suspending processing for `$BLOCK_TIME`ms", e)
            delay(BLOCK_TIME)
        }
    }
}

private suspend fun processEvent(event: ZMQEvent) {

    if (event.type == ZMQEventType.HASH_BLOCK) {
        try {
            var success = false
            for (attempt in 1..5) {
                try {
                    val blockHash = JsonPrimitive(event.payload)
                    block = RPC.getValue<Block>(RPCMethod.GET_BLOCK, blockHash, JsonPrimitive(2))
                    recordTransactions(block!!)
                    success = true
                    break
                } catch (e: Throwable) {
                    val timeout = 6000L * attempt
                    logger.error("Failed to process $event; retrying in `$timeout`ms", e)
                    delay(timeout)
                }
            }

            if (!success) {
                logger.error("Gave up on processing $event after $retries attempts; ${rawTransactions.size} raw transaction(s)  will be dropped as a result")
            }
        } finally {
            rawTransactions.clear()
        }
    }

    if (event.type == ZMQEventType.RAW_TX) {
        val currentBlock = block ?: return

        val hex = event.payload
        if (rawTransactions.containsKey(hex)) return

        val tx = RawZMQTransaction(
            timeReceived = System.currentTimeMillis(),
            blockHeightReceived = currentBlock.height,
            hex = hex,
        )
        rawTransactions[hex] = tx

        if (rawTransactions.size % 100 == 0) {
            logger.info("${rawTransactions.size} raw transactions in queue")
        }
    }
}

private suspend fun decodeRawTransactions(block: Block): List<Triple<RawZMQTransaction, TX, Boolean>> {
    val confirmedTransactionsByTXHex = block.tx.associateBy { it.hex }
    val triples = ArrayList<Triple<RawZMQTransaction, TX, Boolean>>(rawTransactions.size)
    for (rawTX in rawTransactions.values) {

        val isConfirmed = confirmedTransactionsByTXHex.containsKey(rawTX.hex)
        val tx = if(isConfirmed) confirmedTransactionsByTXHex.getValue(rawTX.hex) else RPC.getValue(
            RPCMethod.DECODE_RAW_TRANSACTION,
            JsonPrimitive(rawTX.hex)
        )
        triples.add(Triple(rawTX, tx, isConfirmed))
    }
    return triples
}
private suspend fun recordTransactions(block: Block) {

    val zmqTransactions = ArrayList<ZMQTransaction>()
    val decodedTransactions = decodeRawTransactions(block)
    val txContext = decodedTransactions.associate { it.second.txID to it.second }
    for ((rawTX, tx, isConfirmed) in decodedTransactions) {

        val customTransaction = RPC.decodeCustomTX(rawTX.hex)

        val zmqTX = ZMQTransaction(
            txID = tx.txID,
            type = customTransaction?.type ?: "None",
            fee = calculateFee(tx, txContext),
            blockHeightReceived = rawTX.blockHeightReceived,
            timeReceived = rawTX.timeReceived,
            hex = rawTX.hex,
            isConfirmed = isConfirmed,
            poolSwap = decodePoolSwap(customTransaction),
        )
        zmqTransactions.add(zmqTX)
    }

    val poolSwapCount = zmqTransactions.count { it.poolSwap != null }
    val unconfirmed = zmqTransactions.count { !it.isConfirmed }
    if (poolSwapCount > 0) {
        indexTokens(dbUpdater)
    }

    DB.insertZMQTransactions(dbUpdater, zmqTransactions)
    logger.info("Indexed ${zmqTransactions.size} transaction(s) ($poolSwapCount pool swaps, $unconfirmed unconfirmed) at block height ${block.height}")

    if (poolSwapCount > 0) {
        poolSwapChannel.send(true)
    }
}

data class ZMQTransaction(
    val txID: String,
    val type: String,
    val timeReceived: Long,
    val blockHeightReceived: Int,
    val hex: String,
    val fee: BigDecimal,
    val isConfirmed: Boolean,
    val poolSwap: DB.PoolSwap?
)

private data class RawZMQTransaction(
    val timeReceived: Long,
    val blockHeightReceived: Int,
    val hex: String,
)