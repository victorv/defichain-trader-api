package com.trader.defichain.plugins

import com.trader.defichain.appServerConfig
import com.trader.defichain.db.DB
import com.trader.defichain.db.search.PoolHistoryFilter
import com.trader.defichain.db.search.getMetrics
import com.trader.defichain.db.search.getPoolSwaps
import com.trader.defichain.dex.*
import com.trader.defichain.http.*
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
            connection.sendUUID()

            try {
                launch(Dispatchers.IO) {
                    for (message in incoming) {
                        val message = Json.decodeFromString<Message>(message.data.decodeToString())
                        when (message.id) {
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
                connections -= connection
            }
        }

        get("/status") {
            call.respond(HttpStatusCode.OK, "ok")
        }
        get("/graph") {
            val poolSwap = extractPoolSwap(call) ?: return@get call.respond(BadRequest("?poolSwap=... is missing"))
            val blocks = call.request.queryParameters["blocks"] ?: return@get call.respond(BadRequest("?blocks=... is missing"))
            val graph = getMetrics(poolSwap, blocks.toInt())
            call.respond(graph)
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
        post("/notification") {
            val uuid = call.request.queryParameters["uuid"]!!
            val description = call.request.queryParameters["description"]!!
            check(description.length < 150)

            val filter = call.receive<PoolHistoryFilter>()
            val poolSwaps = getPoolSwaps(filter)
            if (poolSwaps.rows.isEmpty()) {
                call.respond(HttpStatusCode.BadRequest, "does not match any records")
                return@post
            }
            for(connection in connections) {
                if (connection.uuid == uuid) {
                    connection.description = description
                    connection.filter = filter
                    call.respond(HttpStatusCode.OK, "ok")
                    return@post
                }
            }
            call.respond(HttpStatusCode.BadRequest, "invalid UUID")
        }
        get("/stats") {
            val template = call.request.queryParameters["template"]!!
            val period = call.request.queryParameters["period"]!!.toInt()
            val tokensFrom = getTokenIdentifiers(call.request.queryParameters["tokenFrom"])
            val tokensTo = getTokenIdentifiers(call.request.queryParameters["tokenTo"])
            call.respond(DB.stats(template, period, tokensFrom, tokensTo))
        }
        post("/poolswaps") {
            val filter = call.receive<PoolHistoryFilter>()
            val poolSwaps = getPoolSwaps(filter)
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

@kotlinx.serialization.Serializable
data class LoginRequest(
    val loginCode: Long,
)