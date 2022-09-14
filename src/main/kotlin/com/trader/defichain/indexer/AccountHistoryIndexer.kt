package com.trader.defichain.indexer

import com.trader.defichain.db.DB
import com.trader.defichain.rpc.AccountHistory
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext
import kotlin.math.max
import kotlin.math.min

private val logger = LoggerFactory.getLogger("AccountHistoryIndexer")
private val dbUpdater = initialDatabaseUpdater
val poolSwapChannel = Channel<Boolean>(1, BufferOverflow.DROP_OLDEST)

suspend fun indexAccountHistory(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        indexTail()

        var previouslyProcessed: Set<String> = emptySet()
        var minBlockHeight = DB.getLatestPoolSwapBlock(dbUpdater)
        logger.info("Indexing account history (HEAD); catching up until block height $minBlockHeight")
        while (true) {
            val result = indexHead(previouslyProcessed, minBlockHeight)
            previouslyProcessed = result.first
            minBlockHeight = result.second
            poolSwapChannel.receive()
        }
    }
}

private suspend fun indexTail() {
    var alreadyProcessed = emptySet<String>()
    var currentBlockHeight = DB.getOldestPoolSwapBlock(dbUpdater)
    logger.info("Indexing account history (TAIL); catching up down from block height $currentBlockHeight")

    while (true) {
        try {
            val (processed, _, lowestBlockHeight) = indexAccountHistory(alreadyProcessed, currentBlockHeight)
            if (processed.isEmpty() || lowestBlockHeight == currentBlockHeight) {
                break
            }

            currentBlockHeight = lowestBlockHeight
            alreadyProcessed = processed
        } catch (e: Throwable) {
            logger.error("Failed to index account history (TAIL); suspending processing for `$BLOCK_TIME`ms", e)
            delay(BLOCK_TIME)
        }
    }
}

private suspend fun indexHead(alreadyProcessed: Set<String>, minBlockHeight: Int): Pair<Set<String>, Int> {
    var currentBlockHeight = Integer.MAX_VALUE
    var maxBlockHeight = minBlockHeight
    var previouslyProcessed: Set<String> = HashSet(alreadyProcessed)
    while (true) {
        try {
            val (processed, highestBlockHeight, lowestBlockHeight) = indexAccountHistory(
                previouslyProcessed,
                currentBlockHeight
            )
            if (currentBlockHeight < minBlockHeight) {
                maxBlockHeight = max(maxBlockHeight, highestBlockHeight)
                return Pair(processed, maxBlockHeight - 1)
            }

            currentBlockHeight = lowestBlockHeight
            previouslyProcessed = processed
        } catch (e: Throwable) {
            logger.error("Failed to index account history (HEAD); suspending processing for `$BLOCK_TIME`ms", e)
            delay(BLOCK_TIME)
        }
    }
}

private suspend fun indexAccountHistory(
    alreadyProcessed: Set<String>,
    maxBlockHeight: Int
): Triple<Set<String>, Int, Int> {
    val all = JsonPrimitive("all")
    val options = JsonObject(
        mapOf(
            "no_rewards" to JsonPrimitive(true),
            "txtype" to JsonPrimitive("PoolSwap"),
            "limit" to JsonPrimitive(1000),
            "format" to JsonPrimitive("id"),
            "maxBlockHeight" to JsonPrimitive(maxBlockHeight)
        )
    )
    logger.info(options.toString())

    val accountHistory = RPC.getValue<List<AccountHistory.PoolSwap>>(RPCMethod.LIST_ACCOUNT_HISTORY, all, options)
        .filter { !it.owner.contains("defichainBurnAddressXXXXXXX") }.groupBy { it.txid }

    var highestBlockHeight = maxBlockHeight
    var lowestBlockHeight = maxBlockHeight
    val mintedPoolSwaps =
        accountHistory.entries.map { (txID, records) ->
            val amounts = records.map { it.amounts.toList() }.flatten().sorted()

            if (amounts.size != 2) {
                return@map null
            }

            val (amountFromString, tokenFrom) = amounts.first().split("@")
            val amountFrom = amountFromString.toDouble()
            check(amountFrom < 0.0)

            val (amountToString, tokenTo) = amounts.last().split("@")
            val amountTo = amountToString.toDouble()
            check(amountTo >= 0.0)

            val firstRecord = records.first()
            val lastRecord = if (records.size == 2) records.last() else firstRecord

            if (alreadyProcessed.contains(txID)) return@map null
            highestBlockHeight = max(highestBlockHeight, firstRecord.blockHeight)
            lowestBlockHeight = min(lowestBlockHeight, firstRecord.blockHeight)


            val fee = calculateFee(txID)
            val maxPrice = getMaxPrice(txID)

            val mintedTX = DB.MintedTX(
                txID = txID,
                txFee = fee,
                txOrdinal = firstRecord.txn,
                blockHeight = firstRecord.blockHeight,
                blockTime = firstRecord.blockTime,
            )

            val swap = DB.PoolSwap(
                from = firstRecord.owner,
                to = lastRecord.owner,
                tokenFrom = tokenFrom.toInt(),
                tokenTo = tokenTo.toInt(),
                amountFrom = amountFrom,
                amountTo = amountTo,
                maxPrice = maxPrice,
            )
            Pair(mintedTX, swap)

        }.filterNotNull()

    DB.insertMintedPoolSwaps(dbUpdater, mintedPoolSwaps)

    val processed = mintedPoolSwaps.map { it.first.txID }.toSet()
    return Triple(processed, highestBlockHeight, lowestBlockHeight)
}