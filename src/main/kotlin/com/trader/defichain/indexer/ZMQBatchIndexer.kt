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
private val zmqBatchChannel = Channel<ZMQBatch>(20, BufferOverflow.DROP_OLDEST)

suspend fun announceZMQBatch(zmqBatch: ZMQBatch) {
    zmqBatchChannel.send(zmqBatch)
}

suspend fun indexZMQBatches(coroutineContext: CoroutineContext) {
    var missingBlocks = listOf<Int>().iterator()

    while (coroutineContext.isActive) {
        do {
            while (zmqBatchChannel.isEmpty) {
                var blockHeight = -500
                try {
                    if (!missingBlocks.hasNext()) {
                        missingBlocks = DB.selectAll<Int>("missing_blocks").iterator()
                    }

                    blockHeight = missingBlocks.next()

                    val blockHash = RPC.getValue<String>(RPCMethod.GET_BLOCK_HASH, JsonPrimitive(blockHeight))
                    val block = RPC.getValue<Block>(RPCMethod.GET_BLOCK, JsonPrimitive(blockHash), JsonPrimitive(2))

                    val dbtx = DBTX("missing block at block height ${block.height}")
                    indexZMQBatches(dbtx, block)
                    dbtx.submit()

                    logger.info("Indexed missing block at block height ${block.height}")
                } catch (e: Throwable) {
                    logger.error(
                        "Failed to process missing block at height $blockHeight; skipping block and suspending processing until the next ZMQ batch is available",
                        e
                    )
                    break
                }
            }

            val zmqBatch = zmqBatchChannel.receive()
            try {
                val dbtx = DBTX("ZMQ batch at block height ${zmqBatch.block.height}")
                dbtx.indexTokens()
                indexBlock(dbtx, zmqBatch)
                dbtx.submit()

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
    dbtx: DBTX,
    block: Block,
) {
    indexBlock(
        dbtx,
        ZMQBatch(
            block = block,
            tx = block.tx.map { tx -> ZMQPair(null, tx, true) },
            txContext = emptyMap(),
        )
    )
}

private suspend fun indexBlock(dbTX: DBTX, zmqBatch: ZMQBatch) {
    dbTX.insertBlock(zmqBatch.block)

    withContext(dispatcher) {
        zmqBatch.tx.map { zmqPair ->
            async {
                semaphore.withPermit {
                    try {
                        indexZMQPair(zmqPair, zmqBatch, dbTX)
                    } catch (e: Throwable) {
                        throw RuntimeException("Failed to process $zmqPair", e)
                    }
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

    val txRowID = dbTX.insertTX(tx.txID, customTX.type, zmqPair.isConfirmed)

    if (rawTX != null) {
        dbTX.insertRawTX(txRowID, rawTX)
    }

    val txn = tx.txn

    if(zmqPair.isConfirmed) {
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
    }

    if (customTX.isPoolSwap()) {
        val swap = customTX.asPoolSwap()

        if (zmqPair.isConfirmed) {
            swap.amountTo = AccountHistory.getPoolSwapResultFor(swap, block.height, txn)
        }

        dbTX.insertPoolSwap(txRowID, swap)
    } else if (customTX.isAddPoolLiquidity()) {
        val addPoolLiquidity = customTX.asAddPoolLiquidity()

        val poolID = TokenIndex.getPoolID(addPoolLiquidity.tokenA, addPoolLiquidity.tokenB)
        val sharesUnknown = TokenIndex.TokenAmount(
            tokenID = poolID,
            amount = null,
        )

        val shares = if (zmqPair.isConfirmed)
            AccountHistory.getPoolLiquidityShares(addPoolLiquidity, block.height, txn) ?: sharesUnknown
        else sharesUnknown

        dbTX.addPoolLiquidity(txRowID, addPoolLiquidity, shares)
    } else if (customTX.isRemovePoolLiquidity()) {
        val removePoolLiquidity = customTX.asRemovePoolLiquidity()
        val pool = TokenIndex.getPoolPair(removePoolLiquidity.poolID)
        val idTokenA = pool.idTokenA.toInt()
        val idTokenB = pool.idTokenB.toInt()

        val amounts = if (zmqPair.isConfirmed) AccountHistory.getPoolLiquidityAmounts(
            removePoolLiquidity.owner,
            block.height,
            txn,
            idTokenA,
            idTokenB
        ) else AccountHistory.PoolLiquidityAmounts(
            amountA = TokenIndex.TokenAmount(idTokenA, null),
            amountB = TokenIndex.TokenAmount(idTokenB, null),
        )

        dbTX.removePoolLiquidity(txRowID, removePoolLiquidity, amounts)
    }
}