package com.trader.defichain.telegram

import com.trader.defichain.appServerConfig
import com.trader.defichain.db.search.PoolHistoryFilter
import com.trader.defichain.db.search.PoolSwapRow
import com.trader.defichain.http.connections
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.io.path.name
import kotlin.math.roundToInt

val notifications = CopyOnWriteArrayList<Notification>()

fun loadNotifications() {
    Files.list(Paths.get(appServerConfig.accountsRoot)).forEach {
        val contents = Files.readAllBytes(it).decodeToString()
        if (it.name.startsWith("ph-")) {
            val notification = Json.decodeFromString<PoolHistoryNotification>(contents)
            notifications += notification
        }
    }
    println("Loaded ${notifications.size} notifications")
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

    fun write()

    suspend fun delete()

    suspend fun test(value: Any)
    suspend fun sendCard()
    suspend fun checkValidity(): Boolean
}

@kotlinx.serialization.Serializable
data class PoolHistoryNotification(
    override val uuid: String,
    override val description: String,
    override val chatID: Long,
    val filter: PoolHistoryFilter,
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

    override suspend fun test(value: Any) {
        if (value is PoolSwapRow) {
            if (filter.fromTokenSymbol != null && value.tokenFrom != filter.fromTokenSymbol) {
                return
            }
            if (filter.toTokenSymbol != null && value.tokenTo != filter.toTokenSymbol) {
                return
            }

            val fee = BigDecimal(value.fee).toDouble()
            if (filter.minFee != null && fee < filter.minFee) {
                return
            }
            if (filter.maxFee != null && fee > filter.maxFee) {
                return
            }

            val blockHeight = value.block?.blockHeight ?: return
            if (filter.minBlock != null && blockHeight < filter.minBlock) {
                return
            }
            if (filter.maxBlock != null && blockHeight > filter.maxBlock) {
                return
            }

            val input = value.fromAmountUSD
            if (filter.minInputAmount != null && input < filter.minInputAmount) {
                return
            }
            if (filter.maxInputAmount != null && input > filter.maxInputAmount) {
                return
            }

            val output = value.toAmountUSD
            if (filter.minOutputAmount != null && output < filter.minOutputAmount) {
                return
            }
            if (filter.maxOutputAmount != null && output > filter.maxOutputAmount) {
                return
            }

            if (filter.fromAddress != null && value.from != filter.fromAddress) {
                return
            }
            if (filter.toAddress != null && value.to != filter.toAddress) {
                return
            }

            if (filter.fromAddressGroup != null && filter.fromAddressGroup.isNotEmpty()) {
                if (!filter.fromAddressGroup.contains(value.from)) {
                    return
                }
            }

            if (filter.toAddressGroup != null && filter.toAddressGroup.isNotEmpty()) {
                if (!filter.toAddressGroup.contains(value.to)) {
                    return
                }
            }

            var message = "<i>$description</i>\n"
            message += "<i>DEX swap confirmed</i>\n"
            message += "<strong>from:</strong> $${(value.fromAmountUSD * 100.0).roundToInt() / 100.0} ${value.tokenFrom}\n"
            message += "<strong>to:</strong> $${(value.toAmountUSD * 100.0).roundToInt() / 100.0} ${value.tokenTo}\n"
            message += "<strong>from address:</strong> ${value.from}\n"
            message += "<strong>to address:</strong> ${value.to}\n"
            message += "<strong>fee:</strong> ${value.fee} \n"
            message += "<strong>block:</strong> ${blockHeight}\n"
            sendTelegramMessage(chatID, uuid, message, false)
        }
    }

    override fun write() {
        val json = Json.encodeToString(this)
        Files.write(path, json.toByteArray(StandardCharsets.UTF_8))
    }
}