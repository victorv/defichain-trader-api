package com.trader.defichain.plugins

import com.trader.defichain.appServerConfig
import com.trader.defichain.db.DB
import com.trader.defichain.dex.*
import com.trader.defichain.http.*
import com.trader.defichain.mempool.connections
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.http.content.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import java.io.File

fun Application.configureRouting() {
    routing {
        static("/") {
            staticRootFolder = File(appServerConfig.webappRoot)

            files(".")

            default("index.html")
        }

        webSocket("/stream") {
            val connection = Connection()
            connections += connection
            try {
                launch(Dispatchers.IO) {
                    for (message in incoming) {
                        val message = Json.decodeFromString<Message>(message.data.decodeToString())
                        when (message.id) {
                            "uuid" -> {
                                connection.setUUID(message.asUUID())
                            }
                            "add-swap" -> {
                                val swap = message.asPoolSwap()

                                val swapResult = testPoolSwap(swap)
                                val message = Message(
                                    id = "swap-result",
                                    data = Json.encodeToJsonElement(swapResult),
                                )
                                connection.send(Json.encodeToString(message))

                                connection.poolSwaps.add(swap)
                            }
                            "remove-swap" -> {
                                val swap = message.asPoolSwap()
                                val pendingRemoval = connection.poolSwaps.filter {
                                    it.tokenFrom == swap.tokenFrom &&
                                            it.tokenTo == swap.tokenTo &&
                                            it.amountFrom == swap.amountFrom &&
                                            it.desiredResult == swap.desiredResult
                                }
                                connection.poolSwaps.removeAll(pendingRemoval)

                                val message = Message(
                                    id = "swaps-removed",
                                    data = Json.encodeToJsonElement(pendingRemoval)
                                )
                                connection.send(Json.encodeToString(message))
                            }
                        }
                    }
                }
                for (message in connection.channel) {
                    send(message)
                }
            } catch (e: Exception) {
                println(e.localizedMessage)
            } finally {
                println("Removing $connection!")
                connections -= connection
            }
        }

        get("/status") {
            call.respond(HttpStatusCode.OK, "ok")
        }
        get("/graph") {
            val poolSwap = extractPoolSwap(call) ?: return@get
            val metrics = DB.getMetrics(poolSwap)
            call.respond(metrics)
        }
        get("/poolpairs") {
            val response = getCachedPoolPairs()
            call.response.header(HttpHeaders.ContentEncoding, "gzip")
            call.respondBytes(ContentType.Application.Json) { response }
        }
        get("/tokens") {
            val response = getCachedTokens()
            call.response.header(HttpHeaders.ContentEncoding, "gzip")
            call.respondBytes(ContentType.Application.Json) { response }
        }
        get("/tokens/sold-recently") {
            call.respond(DB.tokensSoldRecently())
        }
        get("/tokens/bought-recently") {
            call.respond(DB.tokensBoughtRecently())
        }
        post("/poolswaps") {
            val filter = call.receive<DB.PoolHistoryFilter>()
            val poolSwaps = DB.getPoolSwaps(filter)
            call.respond(poolSwaps)
        }
        get("/clear") {
            val response = getCachedTokens()
            call.response.header(HttpHeaders.ContentEncoding, "gzip")
            call.respondBytes(ContentType.Application.Json) { response }
        }
        get("/estimate") {
            val poolSwap = extractPoolSwap(call) ?: return@get
            val testResult = testPoolSwap(poolSwap)
            call.respond(testResult)
        }
    }
}

private suspend fun extractPoolSwap(call: ApplicationCall): PoolSwap? {
    val poolSwapParam = call.request.queryParameters["poolswap"]
    if (poolSwapParam == null) {
        call.respond(errorPoolswapsMissing)
        return null
    }

    val poolSwap = parsePoolSwap(
        if (poolSwapParam.contains("desiredResult")) poolSwapParam
        else "$poolSwapParam desiredResult 1.0"
    )
    poolSwap.checkDesiredResult()
    return poolSwap
}

private fun parsePoolSwap(requestedPoolSwap: String): PoolSwap {
    println(requestedPoolSwap)
    val (swap, desiredResult) = requestedPoolSwap.split(" desiredResult ")
    val fromAndTo = swap.split(" to ")
    val from = fromAndTo[0].split(" ")
    val (fromAmount, fromTokenSymbol) = from
    val toTokenSymbol = fromAndTo[1]

    return PoolSwap(
        amountFrom = fromAmount.toDouble(),
        tokenFrom = fromTokenSymbol,
        tokenTo = toTokenSymbol,
        desiredResult = desiredResult.toDouble(),
    )
}