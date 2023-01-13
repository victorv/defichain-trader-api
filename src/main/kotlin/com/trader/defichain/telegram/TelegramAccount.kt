package com.trader.defichain.telegram

import com.trader.defichain.appServerConfig
import com.trader.defichain.db.search.PoolHistoryFilter
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.dex.getTokenId
import com.trader.defichain.dex.getTokenIdentifiers
import com.trader.defichain.http.connections
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.io.path.name

val notifications = CopyOnWriteArrayList<Notification>()
private val logger = LoggerFactory.getLogger("telegram")

fun loadNotifications() {
    Files.list(Paths.get(appServerConfig.accountsRoot)).forEach {
        val contents = Files.readAllBytes(it).decodeToString()
        if (it.name.startsWith("ph-")) {
            val notification = Json.decodeFromString<PoolHistoryNotification>(contents)
            notifications += notification
        }
    }
    logger.info("Loaded ${notifications.size} notifications")
}

suspend fun approveNotification(uuid: String, chatID: Long) {
    for (connection in connections) {
        val description = connection.description
        val filter = connection.filter
        if (connection.uuid == uuid && description != null && filter != null) {
            val notification = PoolHistoryNotification(UUID.randomUUID().toString(), description, chatID, filter)
            if (notification.checkValidity()) {
                notification.write()
                notifications += notification

                logger.info("approved alert: $notification")

                sendTelegramMessage(
                    chatID,
                    notification.uuid,
                    "Now active: <strong>${connection.description}</strong>, use the command /list to manage your notifications.",
                    false
                )
            }
            break
        }
    }
}

interface Notification {

    val chatID: Long
    val uuid: String
    val description: String
    val filter: PoolHistoryFilter

    fun write()

    suspend fun delete()

    suspend fun matches(value: Any): Boolean
    suspend fun sendCard()
    suspend fun checkValidity(): Boolean
}

@kotlinx.serialization.Serializable
data class PoolHistoryNotification(
    override val uuid: String,
    override val description: String,
    override val chatID: Long,
    override val filter: PoolHistoryFilter,
) : Notification {

    private val path = Paths.get(appServerConfig.accountsRoot).resolve("ph-$uuid.json")

    private fun doDelete() {
        notifications.removeIf { it.uuid == uuid }
        if (Files.exists(path)) {
            Files.delete(path)
        }
    }

    override suspend fun checkValidity(): Boolean {
        return true
    }

    override suspend fun sendCard() {
        sendTelegramMessage(chatID, uuid, "<strong>${description}</strong>", true)
    }

    override suspend fun delete() {
        doDelete()
        sendTelegramMessage(chatID, uuid, "Notification has been deleted: <strong>$description</strong>", false)
    }

    override suspend fun matches(value: Any): Boolean {
        if (value is PoolSwapRow) {
            if(filter.fromTokenSymbol != null && filter.toTokenSymbol == "is_sold_or_bought") {
                val tokenFrom = getTokenId(value.tokenFrom)
                val tokenTo = getTokenId(value.tokenTo)
                val whitelist = getTokenIdentifiers(filter.fromTokenSymbol)

                if (whitelist.isNotEmpty() && !(whitelist.contains(tokenFrom) || whitelist.contains(tokenTo))) {
                    return false
                }
            } else {
                if (filter.fromTokenSymbol != null) {
                    val tokenFrom = getTokenId(value.tokenFrom)
                    val whitelist = getTokenIdentifiers(filter.fromTokenSymbol)

                    if (whitelist.isNotEmpty() && !whitelist.contains(tokenFrom)) {
                        return false
                    }
                }
                if (filter.toTokenSymbol != null) {
                    val tokenTo = getTokenId(value.tokenTo)
                    val whitelist = getTokenIdentifiers(filter.toTokenSymbol)

                    if (whitelist.isNotEmpty() && !whitelist.contains(tokenTo)) {
                        return false
                    }
                }
            }

            val input = value.fromAmountUSD
            if (filter.minInputAmount != null && input < filter.minInputAmount) {
                return false
            }
            if (filter.maxInputAmount != null && input > filter.maxInputAmount) {
                return false
            }

            val output = value.toAmountUSD
            if (filter.minOutputAmount != null && output < filter.minOutputAmount) {
                return false
            }
            if (filter.maxOutputAmount != null && output > filter.maxOutputAmount) {
                return false
            }

            val fee = BigDecimal(value.fee).toDouble()
            if (filter.minFee != null && fee < filter.minFee) {
                return false
            }
            if (filter.maxFee != null && fee > filter.maxFee) {
                return false
            }

            if (filter.fromAddress != null && value.from != filter.fromAddress) {
                return false
            }
            if (filter.toAddress != null && value.to != filter.toAddress) {
                return false
            }

            if (filter.fromAddressGroup != null && filter.fromAddressGroup.isNotEmpty()) {
                if (!filter.fromAddressGroup.contains(value.from)) {
                    return false
                }
            }

            if (filter.toAddressGroup != null && filter.toAddressGroup.isNotEmpty()) {
                if (!filter.toAddressGroup.contains(value.to)) {
                    return false
                }
            }
        }
        return true
    }

    override fun write() {
        val json = Json.encodeToString(this)
        Files.write(path, json.toByteArray(StandardCharsets.UTF_8))
    }
}