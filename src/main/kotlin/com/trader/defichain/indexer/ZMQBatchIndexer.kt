package com.trader.defichain.indexer

import com.trader.defichain.db.*
import com.trader.defichain.rpc.AccountHistory
import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

private val semaphore = Semaphore(6)
private val dispatcher = newSingleThreadContext("ZMQBatchIndexer")
private val logger = LoggerFactory.getLogger("ZMQBatchIndexer")
private val dbUpdater = initialDatabaseUpdater
private val zmqBatchChannel = Channel<ZMQBatch>(20, BufferOverflow.DROP_OLDEST)

suspend fun announceZMQBatch(zmqBatch: ZMQBatch) {
    zmqBatchChannel.send(zmqBatch)
}

suspend fun indexZMQBatches(coroutineContext: CoroutineContext) {
    var blockHash = RPC.getValue<String>(RPCMethod.GET_BEST_BLOCK_HASH)

    while (coroutineContext.isActive) {
        do {
            while (zmqBatchChannel.isEmpty) {
                var block: Block? = null
                try {
                    block = RPC.getValue(RPCMethod.GET_BLOCK, JsonPrimitive(blockHash), JsonPrimitive(2))
                    blockHash = indexZMQBatches(block!!)
                } catch (e: Throwable) {
                    logger.error(
                        "Failed to process old block with hash $blockHash; skipping block and suspending processing until the next ZMQ batch is available",
                        e
                    )
                    blockHash = block?.previousBlockHash ?: blockHash
                    break
                }
            }

            val zmqBatch = zmqBatchChannel.receive()
            try {
                dbUpdater.doTransaction {
                    indexTokens(it)
                    indexBlock(it, zmqBatch)
                }
                logger.info("Indexed ZMQ batch at block height ${zmqBatch.block.height}")
            } catch (e: Throwable) {
                logger.error(
                    "Failed to process ZMQ batch at block height ${zmqBatch.block.height}; dropped batch and suspending processing for ${BLOCK_TIME}ms",
                    e
                )
                delay(BLOCK_TIME)
            }
        } while (!zmqBatchChannel.isEmpty)
    }
}

private val whitelistedTXTypes = setOf(
    "PoolSwap",
    "AddPoolLiquidity",
    "RemovePoolLiquidity",
)

private suspend fun indexZMQBatches(
    block: Block,
): String {
    dbUpdater.doTransaction { dbTX ->
        indexBlock(
            dbTX,
            ZMQBatch(
                block = block,
                tx = block.tx.map { tx -> ZMQPair(null, tx, true) },
                txContext = emptyMap(),
            )
        )
    }
    logger.info("Indexed old block at block height ${block.height}")
    return block.previousBlockHash ?: RPC.getValue(RPCMethod.GET_BEST_BLOCK_HASH)
}

private suspend fun indexBlock(dbTX: DBTX, zmqBatch: ZMQBatch) {
    dbTX.insertBlock(zmqBatch.block)

    withContext(dispatcher) {
        zmqBatch.tx.map { zmqPair ->
            async {
                semaphore.withPermit {
                    indexZMQPair(zmqPair, zmqBatch, dbTX)
                }
            }
        }.awaitAll()
    }
}

private suspend fun indexZMQPair(
    zmqPair: ZMQPair,
    zmqBatch: ZMQBatch,
    dbTX: DBTX
) {
    val tx = zmqPair.tx
    val rawTX = zmqPair.rawTX
    val block = zmqBatch.block

    val customTX = RPC.decodeCustomTX(tx.hex!!) ?: return
    if (!whitelistedTXTypes.contains(customTX.type)) return

    val txRowID = dbTX.insertTX(tx.txID, customTX.type)

    if (rawTX != null) {
        dbTX.insertRawTX(txRowID, rawTX)
    }

    if (!zmqPair.isConfirmed) {
        return
    }

    val txn = tx.txn
    val fee = calculateFee(tx, zmqBatch.txContext)
    val mintedTX = DB.MintedTX(
        txID = tx.txID,
        type = customTX.type,
        txFee = fee,
        txn = txn,
        blockHeight = block.height,
        blockTime = block.time,
    )

    dbTX.insertMintedTX(txRowID, mintedTX)

    if (customTX.isPoolSwap()) {
        val swap = customTX.asPoolSwap()
        swap.amountTo = AccountHistory.getPoolSwapResultFor(swap, block.height, txn)
        dbTX.insertPoolSwap(txRowID, swap)
    } else if (customTX.isAddPoolLiquidity()) {
        val addPoolLiquidity = customTX.asAddPoolLiquidity()
        val amounts = AccountHistory.getPoolLiquidityShares(addPoolLiquidity.owner, block.height, txn)
        // println("$amounts $addPoolLiquidity") TODO store in DB
    } else if (customTX.isRemovePoolLiquidity()) {
        val removePoolLiquidity = customTX.asRemovePoolLiquidity()
        val amounts = AccountHistory.getPoolLiquidityAmounts(
            removePoolLiquidity.owner,
            block.height,
            txn,
            removePoolLiquidity.poolID
        )
        // println("$amounts $removePoolLiquidity") TODO store in DB
    }
}