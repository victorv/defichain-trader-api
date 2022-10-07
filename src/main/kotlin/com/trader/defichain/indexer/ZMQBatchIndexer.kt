package com.trader.defichain.indexer

import com.trader.defichain.db.auctionBid
import com.trader.defichain.db.*
import com.trader.defichain.dex.getPool
import com.trader.defichain.dex.getPoolID
import com.trader.defichain.rpc.AccountHistory
import com.trader.defichain.rpc.Block
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

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

                    if (missingBlocks.hasNext()) {
                        blockHeight = missingBlocks.next()

                        val blockHash = RPC.getValue<String>(RPCMethod.GET_BLOCK_HASH, JsonPrimitive(blockHeight))
                        val block = RPC.getValue<Block>(RPCMethod.GET_BLOCK, JsonPrimitive(blockHash), JsonPrimitive(2))

                        val dbtx = DBTX("missing block at block height ${block.height}")
                        indexZMQBatches(dbtx, block)
                        dbtx.submit()

                        logger.info("Indexed missing block at block height ${block.height}")
                    } else {
                        break
                    }
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
    "DepositToVault",
    "WithdrawFromVault",
    "TakeLoan",
    "PaybackLoan",
    "AuctionBid",
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

    for (zmqPair in zmqBatch.tx) {
        try {
            indexZMQPair(zmqPair, zmqBatch, dbTX)
        } catch (e: Throwable) {
            throw RuntimeException("Failed to process $zmqPair", e)
        }
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

    val fee = calculateFee(tx, zmqBatch.txContext)
    val txRowID = dbTX.insertTX(tx.txID, customTX.type, fee, zmqPair.isConfirmed, customTX.valid)

    if (rawTX != null) {
        dbTX.insertRawTX(txRowID, rawTX)
    }

    val txn = tx.txn

    if (zmqPair.isConfirmed) {
        val mintedTX = DB.MintedTX(
            txID = tx.txID,
            type = customTX.type,
            txn = txn,
            blockHeight = block.height,
            blockTime = block.time,
        )

        dbTX.insertMintedTX(txRowID, mintedTX)
    }

    if (customTX.isPoolSwap()) {
        val swap = customTX.asPoolSwap()

        if (zmqPair.isConfirmed) {
            val tokenAmount = AccountHistory.getPoolSwapResultFor(swap, block.height, txn)
            if (tokenAmount != null) {
                swap.amountTo = tokenAmount
            }
        }

        dbTX.insertPoolSwap(txRowID, swap)
    } else if (customTX.isAddPoolLiquidity()) {
        val addPoolLiquidity = customTX.asAddPoolLiquidity()

        val poolID = getPoolID(addPoolLiquidity.tokenA, addPoolLiquidity.tokenB)
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
        val pool = getPool(removePoolLiquidity.poolID)
        val idTokenA = pool.idTokenA
        val idTokenB = pool.idTokenB

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
    } else if (customTX.isDepositToVault()) {
        dbTX.storeLoanOrCollateral(txRowID, "collateral", customTX.asDepositToVault())
    } else if (customTX.isWithdrawFromVault()) {
        dbTX.storeLoanOrCollateral(txRowID, "collateral", customTX.asWithdrawFromVault())
    } else if (customTX.isTakeLoan()) {
        dbTX.storeLoanOrCollateral(txRowID, "loan", customTX.asTakeLoan())
    } else if (customTX.isPaybackLoan()) {
        dbTX.storeLoanOrCollateral(txRowID, "loan", customTX.asPaybackLoan())
    } else if (customTX.isAuctionBid()) {
        dbTX.auctionBid(txRowID, customTX.asAuctionBid())
    }
}