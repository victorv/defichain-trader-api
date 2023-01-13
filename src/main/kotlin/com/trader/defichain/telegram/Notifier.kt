package com.trader.defichain.telegram

import com.trader.defichain.db.search.PoolHistoryFilter
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.db.search.getPoolSwaps
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.zmq.newZQMBlockChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import java.util.*
import kotlin.coroutines.CoroutineContext
import kotlin.math.min
import kotlin.math.roundToInt

private var blockHeight = runBlocking { RPC.getValue<Int>(RPCMethod.GET_BLOCK_COUNT) }
private val blockChannel = newZQMBlockChannel()
private const val matchLimit = 10

suspend fun notifyTelegramSubscribers(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        try {
            broadcast()
            blockChannel.receive()
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
    ).rows
    val newBlockHeight = rows.maxOfOrNull { it.block?.blockHeight ?: blockHeight } ?: blockHeight
    for ((chatID, group) in notifications.groupBy { it.chatID }) {
        val sections = ArrayList<List<String>>()
        try {
            for (notification in group) {
                if (!notification.checkValidity()) {
                    continue
                }

                val blocks = TreeSet<Int>()
                val matches = mutableListOf<PoolSwapRow>()
                for (row in rows) {
                    if (row.block == null) continue

                    if (notification.matches(row)) {
                        matches.add(row)
                        blocks += row.block!!.blockHeight
                    }
                }
                if (matches.isNotEmpty()) {
                    sections.add(createMessage(matches, blocks, notification))
                }
            }

            if(sections.isNotEmpty()) {
                val message = sections.joinToString("\n-\n") { it.joinToString("\n") }
                sendTelegramMessage(chatID, "", message, false)
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    }
    blockHeight = newBlockHeight
}

private fun createMessage(
    matches: List<PoolSwapRow>,
    blocks: Set<Int>,
    notification: Notification
): List<String> {
    val message =
        mutableListOf("""<strong>${notification.description}</strong>""")
    for (match in matches.sortedByDescending { it.fromAmountUSD }
        .slice(IntRange(0, min(matchLimit - 1, matches.size - 1)))) {
        val amountFromUSD = (match.fromAmountUSD * 100.0).roundToInt() / 100.0
        val amountToUSD = (match.toAmountUSD * 100.0).roundToInt() / 100.0
        message += """$$amountFromUSD ${match.tokenFrom} to $${amountToUSD} ${match.tokenTo} <a href="https://defiscan.live/transactions/${match.txID}">defiscan</a>"""
    }

    if (matches.size > matchLimit) {
        val skipped = message.size - matchLimit
        val verb = if (skipped == 1) "match is" else "matches are"
        message += "&lt;$skipped $verb not displayed&gt;"
    }

    val blockLinks = blocks.map { """<a href="https://defiscan.live/blocks/$it">$it</a>""" }
    message += if (blocks.size == 1) {
        """<strong>block:</strong> ${blockLinks.first()}"""
    } else {
        if (blocks.size > 3) {
            """<strong>blocks:</strong> ${blockLinks.first()} - ${blockLinks.last()}"""
        } else {
            """<strong>blocks:</strong> ${blockLinks.joinToString(", ")}"""
        }
    }
    return message
}