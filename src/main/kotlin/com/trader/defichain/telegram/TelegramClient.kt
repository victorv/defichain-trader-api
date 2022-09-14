package com.trader.defichain.telegram

import com.trader.defichain.appServerConfig
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNames
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

suspend fun sendTelegramMessage(chatId: Long, text: String) {
    val encodedText = URLEncoder.encode(text, StandardCharsets.UTF_8)
    val url = appServerConfig.telegramBot.createURL("sendMessage?chat_id=${chatId}&text=$encodedText&parse_mode=markdown")
    telegramClient.post(url)
}

suspend fun getChat(chatId: Long): TelegramChat? {
    val url = appServerConfig.telegramBot.createURL("getChat?chat_id=${chatId}")
    val response = telegramClient.get(url)
    if (response.status.isSuccess()) {
        val response = response.body<TelegramChatResponse>()
        if (!response.isValid()) {
            return null
        }
        return response.result
    }
    if (response.status == HttpStatusCode.NotFound) {
        return null
    }
    val error = response.body<String>()
    throw IllegalStateException("Failed to get $url: $error")
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
data class TelegramChatResponse(
    val ok: Boolean,
    val result: TelegramChat?,
) {
    fun isValid() = ok && result != null
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
    fun isValid() = text != null && text.startsWith("/start ")
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
data class TelegramUpdate(
    @JsonNames("update_id")
    val updateID: Long,
    val message: TelegramMessage? = null,
) {
    fun isValid() = updateID > 0 && message != null && message.isValid()

    fun asValidTelegramUpdate(): ValidTelegramUpdate {
        check(message != null && isValid())
        return ValidTelegramUpdate(
            updateID = updateID,
            message = message.asValidTelegramMessage()
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
