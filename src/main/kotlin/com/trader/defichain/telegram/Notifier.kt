package com.trader.defichain.telegram

import com.trader.defichain.db.search.PoolHistoryFilter
import com.trader.defichain.db.search.getPoolSwaps
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlin.coroutines.CoroutineContext

private var blockHeight = runBlocking { RPC.getValue<Int>(RPCMethod.GET_BLOCK_COUNT) }

suspend fun notifyTelegramSubscribers(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        try {
            broadcast()
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    }
}

private suspend fun broadcast() {
    val rows = getPoolSwaps(
        PoolHistoryFilter(
            minBlock = blockHeight + 1
        )
    )
    val newBlockHeight = rows.maxOfOrNull { it.block?.blockHeight ?: blockHeight } ?: blockHeight
    for (notification in notifications) {
        try {
            for (row in rows) {
                notification.test(row)
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    }

    if (blockHeight == newBlockHeight) {
        delay(5000)
    }
    blockHeight = newBlockHeight
}