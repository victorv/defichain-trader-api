package com.trader.defichain.indexer

import com.trader.defichain.db.*
import com.trader.defichain.dex.*
import com.trader.defichain.rpc.*
import com.trader.defichain.util.joinVersions
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
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
                        missingBlocks = DB.selectAll<Int>("select * from blocks_not_finalized").iterator()
                    }
                    if (!missingBlocks.hasNext()) {
                        missingBlocks = DB.selectAll<Int>("select * from missing_blocks").iterator()
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
    val masterNodeTX = RPC.getMasterNodeTX(zmqBatch.block.masterNode)
    val masterNode =
        dbTX.insertTX(
            masterNodeTX.tx.txID,
            masterNodeTX.type,
            masterNodeTX.fee,
            masterNodeTX.tx.vsize,
            isConfirmed = true,
            valid = true
        )
    dbTX.insertBlock(zmqBatch.block, masterNode, true)

    for (zmqPair in zmqBatch.tx) {
        try {
            indexZMQPair(zmqPair, zmqBatch, dbTX)
        } catch (e: Throwable) {
            throw RuntimeException("Failed to process $zmqPair", e)
        }
    }
}

private fun getSwapPath(swap: CustomTX.PoolSwap, customTX: CustomTX.Record): Int {
    val compositeDex = customTX.results["compositeDex"]
    val fromToken = getTokenSymbol(swap.fromToken)
    val toToken = getTokenSymbol(swap.toToken)
    if (compositeDex != null) {
        return compositeDex.jsonPrimitive.content
            .split("/")
            .joinVersions()
            .map {
                val (tokenSymbolA, tokenSymbolB) = it.split("-")
                val tokenIdA = getTokenId(tokenSymbolA)
                val tokenIdB = getTokenId(tokenSymbolB)
                if (tokenIdA != null && tokenIdB != null) {
                    return getPoolID(tokenIdA, tokenIdB)
                }
                getPoolID(it)
            }.hashCode()
    }

    val paths = getSwapPaths(
        PoolSwap(
            tokenFrom = fromToken,
            tokenTo = toToken,
            amountFrom = 1.0,
        )
    )
    if (paths.size == 1) {
        return paths[0].hashCode()
    }

    return getPoolID(swap.fromToken, swap.toToken)
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
    val txRowID = dbTX.insertTX(tx.txID, customTX.type, fee, tx.vsize, zmqPair.isConfirmed, customTX.valid)

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
            blockTime = block.medianTime,
        )

        dbTX.insertMintedTX(txRowID, mintedTX)
    }

    if (customTX.isPoolSwap()) {
        val swap = customTX.asPoolSwap()
        if (zmqPair.isConfirmed) {
            val tokenAmount = AccountHistory.getPoolSwapResultFor(swap, block.height, txn)
            if (tokenAmount != null) {
                swap.amountTo = tokenAmount
            } else {
                dbTX.isDirty = true
            }
            swap.path = getSwapPath(swap, customTX)
        }

        dbTX.insertPoolSwap(txRowID, swap)
    } else if (customTX.isAddPoolLiquidity()) {
        val addPoolLiquidity = customTX.asAddPoolLiquidity()

        val poolID = getPoolID(addPoolLiquidity.tokenA, addPoolLiquidity.tokenB)
        val sharesUnknown = TokenIndex.TokenAmount(
            tokenID = poolID,
            amount = null,
        )

        var shares = sharesUnknown
        if (zmqPair.isConfirmed) {
            val v = AccountHistory.getPoolLiquidityShares(addPoolLiquidity, block.height, txn)
            if (v != null) {
                shares = v
            } else {
                dbTX.isDirty = true
            }
        }

        dbTX.addPoolLiquidity(txRowID, addPoolLiquidity, shares)
    } else if (customTX.isRemovePoolLiquidity()) {
        val removePoolLiquidity = customTX.asRemovePoolLiquidity()
        val pool = getPool(removePoolLiquidity.poolID)
        val idTokenA = pool.idTokenA
        val idTokenB = pool.idTokenB

        var amounts = AccountHistory.PoolLiquidityAmounts(
            amountA = TokenIndex.TokenAmount(idTokenA, null),
            amountB = TokenIndex.TokenAmount(idTokenB, null),
        )
        if (zmqPair.isConfirmed) {
            val v = AccountHistory.getPoolLiquidityAmounts(
                removePoolLiquidity.owner,
                block.height,
                txn,
                idTokenA,
                idTokenB
            )
            if (v != null) {
                amounts = v
            } else {
                dbTX.isDirty = true
            }
        }

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