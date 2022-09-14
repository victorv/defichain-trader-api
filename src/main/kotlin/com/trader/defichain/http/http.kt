package com.trader.defichain.http

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
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