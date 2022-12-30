package com.trader.defichain.telegram

import com.trader.defichain.db.search.PoolHistoryFilter
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.db.search.getPoolSwaps
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import java.util.TreeSet
import kotlin.coroutines.CoroutineContext
import kotlin.math.min
import kotlin.math.roundToInt

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
        ),
        false
    )
    val newBlockHeight = rows.maxOfOrNull { it.block?.blockHeight ?: blockHeight } ?: blockHeight
    for (notification in notifications) {
        try {
            val filter = notification.filter
            if (!notification.checkValidity()) {
                continue
            }

            val blocks = TreeSet<Int>()
            val matches = mutableListOf<PoolSwapRow>()
            var sumInputAmount = 0.0
            for (row in rows) {
                if (row.block == null) continue

                if (notification.matches(row)) {
                    if (filter.minInputAmount == null || row.fromAmountUSD >= filter.minInputAmount) {
                        matches.add(row)
                    } else {
                        sumInputAmount += row.fromAmountUSD
                    }
                    blocks += row.block!!.blockHeight
                }
            }
            if (matches.isNotEmpty() || (filter.minInputAmount != null && sumInputAmount >= filter.minInputAmount)) {
                send(matches, (sumInputAmount * 100.0).roundToInt() / 100.0, blocks, notification)
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

private suspend fun send(matches: List<PoolSwapRow>, sumInputAmount: Double, blocks: Set<Int>, notification: Notification) {
    val message =
        mutableListOf("""<strong>${notification.description}</strong>""")
    for (match in matches.sortedByDescending { it.fromAmountUSD }.slice(IntRange(0, min(19, matches.size - 1)))) {
        val amountFromUSD = (match.fromAmountUSD * 100.0).roundToInt() / 100.0
        val amountToUSD = (match.toAmountUSD * 100.0).roundToInt() / 100.0
        message += """$$amountFromUSD ${match.tokenFrom} to $${amountToUSD} ${match.tokenTo} <a href="https://defiscan.live/transactions/${match.txID}">defiscan</a>"""
    }
    if (matches.size == 1) {
        val match = matches[0]
        if (match.from != match.to) {
            message += """<strong>from address:</strong> <a href="https://defiscan.live/address/${match.from}">${match.from}</a>"""
            message += """<strong>to address:</strong> <a href="https://defiscan.live/address/${match.to}">${match.to}</a>"""
        } else {
            message += """<strong>from/to:</strong> <a href="https://defiscan.live/address/${match.from}">${match.from}</a>"""
        }
    } else if (matches.size > 20) {
        message += "Too many matching transactions were found, ${message.size - 20} matches have been ignored"
    }

    if (sumInputAmount != 0.0) {
        message += if (notification.filter.minInputAmount == null) {
            "sum of all input amounts: $sumInputAmount USDT"
        } else {
            "sum of other input amounts (&lt; ${notification.filter.minInputAmount} USDT): $sumInputAmount USDT"
        }
    }

    message += if (blocks.size == 1) {
        val height = blocks.first()
        """<strong>block:</strong> <a href="https://defiscan.live/blocks/$height">${height}</a>"""
    } else {
        if (blocks.size > 5) {
            """<strong>blocks:</strong> ${blocks.first()} - ${blocks.last()}"""
        } else {
            """<strong>blocks:</strong> ${blocks.joinToString(", ")}"""
        }
    }
    sendTelegramMessage(notification.chatID, notification.uuid, message.joinToString("\n"), false)
}