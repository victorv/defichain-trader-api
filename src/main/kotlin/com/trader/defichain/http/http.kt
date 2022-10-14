package com.trader.defichain.http

import com.trader.defichain.dex.PoolSwap
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import java.util.zip.GZIPOutputStream

const val poolswapLimit = 10

val errorPoolswapsMissing = BadRequest("Specify at least one PoolSwap.")
val errorPoolSwapLimitExceeded = BadRequest("PoolSwap limit exceeded.")

@kotlinx.serialization.Serializable
data class HttpError(val error: String)

@kotlinx.serialization.Serializable
data class BadRequest(val message: String) {
    val data = Json.encodeToString(HttpError(message)).toByteArray(StandardCharsets.UTF_8)
}

suspend fun ApplicationCall.respond(error: BadRequest) {
    this.respondBytes(error.data, ContentType.Application.Json, HttpStatusCode.BadRequest)
}

inline fun <reified T> gzip(data: T): ByteArray {
    val encoded = Json.encodeToString(data).toByteArray(StandardCharsets.UTF_8)
    val buffer = ByteArrayOutputStream(encoded.size)
    GZIPOutputStream(buffer, encoded.size).use {
        it.write(encoded)
        it.flush()
    }
    return buffer.toByteArray()
}

@kotlinx.serialization.Serializable
data class Message(
    val id: String,
    val data: JsonElement,
) {
    fun asSetGraph() = Json.decodeFromJsonElement<SetGraph>(data)
    fun asPoolSwap() = Json.decodeFromJsonElement<PoolSwap>(data)
    fun asUUID() = Json.decodeFromJsonElement<String>(data)
}

@kotlinx.serialization.Serializable
data class SetGraph(val fromToken: String, val toToken: String)