package com.trader.defichain.telegram

import com.trader.defichain.appServerConfig
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

private val messageQueue = Channel<JsonObject>(900, BufferOverflow.DROP_OLDEST)
private const val messagingRate = 1000L / 30L
private val logger = LoggerFactory.getLogger("telegram")

suspend fun broadcastTelegramMessages(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        val message = messageQueue.receive()
        try {
            val chatID = message["chat_id"].toString()

            val url =
                appServerConfig.telegramBot.createURL("")
            telegramClient.post(url) {
                contentType(ContentType.Application.Json)
                setBody(message)
            }

            logger.info("send alert to $chatID")
        } catch (e: Throwable) {
            logger.error("failed to send alert $message", e)
        } finally {
            delay(messagingRate)
        }
    }
}

suspend fun sendTelegramMessage(chatId: Long, uuid: String, text: String, includeReplyMarkup: Boolean = true) {
    val keyboard = JsonObject(
        mapOf(
            "inline_keyboard" to JsonArray(
                listOf(
                    JsonArray(
                        listOf(
                            JsonObject(
                                mapOf(
                                    "text" to JsonPrimitive("Unsubscribe"),
                                    "callback_data" to JsonPrimitive("delete $uuid")
                                )
                            )
                        )
                    )
                )
            )
        )
    )

    val properties = mutableMapOf<String, JsonElement>(
        "method" to JsonPrimitive("sendMessage"),
        "chat_id" to JsonPrimitive(chatId.toString()),
        "text" to JsonPrimitive(text),
        "parse_mode" to JsonPrimitive("HTML"),
    )

    if (includeReplyMarkup) {
        properties["reply_markup"] = keyboard
    }

    messageQueue.send(JsonObject(properties))
}

suspend fun getTelegramUpdates(offset: Long): TelegramUpdates {
    val url =
        appServerConfig.telegramBot.createURL("getUpdates?timeout=300&limit=20&offset=${offset}&allowed_updates=bot_command")
    return telegramClient
        .get(url)
        .body()
}

val telegramClient = HttpClient(Java) {
    install(ContentNegotiation) {
        json(Json {
            ignoreUnknownKeys = true
        })
    }
}

@kotlinx.serialization.Serializable
data class TelegramChat(
    val id: Long
)

@kotlinx.serialization.Serializable
data class TelegramFrom(
    val id: Long,
    @JsonNames("is_bot")
    val isBot: Boolean,
)

data class ValidTelegramMessage(
    val text: String,
    val from: TelegramFrom,
    val chat: TelegramChat,
)

@kotlinx.serialization.Serializable
data class TelegramMessage(
    val text: String? = null,
    val from: TelegramFrom? = null,
    val chat: TelegramChat? = null,
) {
    fun isValid() = text != null && (text.startsWith("/start ") || text.startsWith("/list"))
            && from != null && from.id > 0 && !from.isBot
            && chat != null && chat.id > 0

    fun asValidTelegramMessage(): ValidTelegramMessage {
        check(text != null && from != null && chat != null && isValid())
        return ValidTelegramMessage(
            text,
            from,
            chat
        )
    }
}

data class ValidTelegramUpdate(
    val updateID: Long,
    val message: ValidTelegramMessage,
)

@kotlinx.serialization.Serializable
data class CallbackQuery(
    val data: String? = null,
    val message: TelegramMessage? = null
) {
    fun isValid() = data != null && message != null && data.startsWith("delete ")

    fun asValidTelegramMessage(): ValidTelegramMessage {
        return ValidTelegramMessage(
            data!!,
            message!!.from!!,
            message!!.chat!!
        )
    }
}

@kotlinx.serialization.Serializable
data class TelegramUpdate(
    @JsonNames("update_id")
    val updateID: Long,
    val message: TelegramMessage? = null,
    @JsonNames("callback_query")
    val callbackQuery: CallbackQuery? = null,
) {
    fun isValid() =
        updateID > 0 && ((message != null && message.isValid()) || (callbackQuery != null && callbackQuery.isValid()))

    fun asValidTelegramUpdate(): ValidTelegramUpdate {
        check(isValid())
        val valid = message?.asValidTelegramMessage() ?: callbackQuery!!.asValidTelegramMessage()

        return ValidTelegramUpdate(
            updateID = updateID,
            message = valid
        )
    }
}

@kotlinx.serialization.Serializable
data class TelegramUpdates(
    val ok: Boolean,
    val result: List<TelegramUpdate>? = null,
) {
    fun getValidUpdates(): List<ValidTelegramUpdate> {
        if (!ok || result == null) {
            return emptyList()
        }
        return result.filter { it.isValid() }.map { it.asValidTelegramUpdate() }
    }

    fun lastUpdateID(): Long {
        if (!ok || result == null || result.isEmpty()) {
            return 0
        }
        return result.last().updateID
    }
}
