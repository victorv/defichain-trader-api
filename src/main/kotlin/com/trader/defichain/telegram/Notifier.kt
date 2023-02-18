package com.trader.defichain.telegram

import com.trader.defichain.appServerConfig
import com.trader.defichain.db.search.DataType
import com.trader.defichain.db.search.SearchFilter
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.db.search.searchPoolSwaps
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.util.asUSDT
import com.trader.defichain.util.round
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.coroutines.CoroutineContext
import kotlin.math.min

private var blockHeight = runBlocking { RPC.getValue<Int>(RPCMethod.GET_BLOCK_COUNT) }
private val logger = LoggerFactory.getLogger("telegram")
private const val matchLimit = 10

suspend fun notifyTelegramSubscribers(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        try {
            broadcast()
        } catch (e: Throwable) {
            logger.error("error while testing notifications", e)
        } finally {
            delay(3000)
        }
    }
}

private suspend fun broadcast() {
    val rows = searchPoolSwaps(
        SearchFilter(
            minBlock = blockHeight + 1
        ),
        DataType.LIST,
        500
    ).results
    val newBlockHeight = rows.maxOfOrNull { it.block?.blockHeight ?: blockHeight } ?: blockHeight
    var sentCount = 0
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
                        blocks += row.block.blockHeight
                    }
                }
                if (matches.isNotEmpty()) {
                    sections.add(createMessage(matches, blocks, notification))
                }
            }

            if(sections.isNotEmpty()) {
                val message = sections.joinToString("\n-\n") { it.joinToString("\n") }
                sendTelegramMessage(chatID, "", message, false)
                sentCount++
            }
        } catch (e: Throwable) {
            logger.error("error while testing notifications of $chatID", e)
        }
    }
    logger.info("tested ${notifications.size} against ${rows.size} results in block range [$blockHeight - $newBlockHeight] resulting in $sentCount scheduled alerts")

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
        val amountFromUSD = asUSDT(match.amountFrom.toDouble(), match.tokenFrom)
        val amountFrom = round(match.amountFrom)
        val amountTo = round(match.amountTo ?: "0.0")

        message += """$amountFrom ${match.tokenFrom} to $amountTo ${match.tokenTo}"""
        message += """$$amountFromUSD <a href="https://defiscan.live/transactions/${match.txID}">defiscan</a>"""
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

    val domain = if(appServerConfig.local) "http://127.0.0.1:8085" else "https://defichain-trader.com"
    val blockRange = "${blocks.first()}-${blocks.last()}"
    val url = "$domain?uuid=${notification.uuid}&blockRange=${blockRange}#explore/PoolSwap"
    message += """<a href="$url">view all</a>"""
    return message
}