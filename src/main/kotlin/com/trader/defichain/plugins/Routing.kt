package com.trader.defichain.plugins

import com.trader.defichain.appServerConfig
import com.trader.defichain.auction.listAuctions
import com.trader.defichain.db.search.*
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.getCachedPoolPairs
import com.trader.defichain.dex.getCachedTokens
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.http.*
import com.trader.defichain.rpc.RPC
import com.trader.defichain.telegram.notifications
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
import kotlinx.serialization.json.JsonObject
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
            val time =
                call.request.queryParameters["time"] ?: return@get call.respond(BadRequest("?time=... is missing"))
            val path =
                call.request.queryParameters["path"] ?: return@get call.respond(BadRequest("?path=... is missing"))
            val graph = getMetrics(poolSwap, time.toLong(), path.toInt())
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
        get("/auctions") {
            val auctions = listAuctions()
            call.respond(auctions)
        }
        get("/account") {
            val address = call.request.queryParameters["address"]!!
            call.respond(RPC.getAccount(address))
        }
        post("/notification") {
            val uuid = call.request.queryParameters["uuid"]!!
            val description = call.request.queryParameters["description"]!!
            check(description.length < 150)

            val filter = call.receive<SearchFilter>()
            val search = searchPoolSwaps(filter, DataType.LIST, 1)
            if (search.results.isEmpty()) {
                call.respond(HttpStatusCode.BadRequest, "does not match any records")
                return@post
            }

            if (setFilter(uuid, description, filter)) {
                call.respond(HttpStatusCode.OK, "ok")
                return@post
            }

            call.respond(HttpStatusCode.BadRequest, "invalid UUID")
        }
        post("/stats") {
            val byAddress = call.parameters["byAddress"] == "true"
            val filter = call.receive<SearchFilter>()
            val stats = if (byAddress) getStatsByAddress(filter) else getStats(filter)
            call.respond(stats)
        }
        get("/download") {
            val uuid = call.request.queryParameters["uuid"]
            val connection = connections.find { it.uuid == uuid }
            if (connection != null) {
                val filter = connection.filter
                if (filter != null) {
                    val csv = when(filter.id) {
                        "PoolSwap" -> getPoolSwapsAsCSV(filter)
                        "PoolLiquidity" -> getPoolLiquidityAsCSV(filter)
                        "Loan" -> getLoansAsCSV(filter)
                        else -> ""
                    }
                    call.respondText(csv, ContentType.Text.CSV)
                    return@get
                }
            }
            call.respondText(
                "<h1>This download link is no longer valid</h1>",
                ContentType.Text.Html,
                HttpStatusCode.BadRequest
            )
        }
        post("update") {
            val uuid = call.request.queryParameters["uuid"]!!
            val filter = call.receive<SearchFilter>()
            setFilter(uuid, "#search-filter", filter)
            call.respond(HttpStatusCode.OK)
        }
        get("filter") {
            val uuid = call.request.queryParameters["uuid"]
            val blockRange = call.request.queryParameters["blockRange"]
            if (uuid != null && blockRange != null) {
                val (minBlockHeight, maxBlockHeight) = blockRange.split("-")
                for (notification in notifications) {
                    if (notification.uuid == uuid) {
                        val filter = notification.filter
                        call.respond(filter.copy(
                            minBlock = minBlockHeight.toInt(),
                            maxBlock = maxBlockHeight.toInt(),
                        ))
                        return@get
                    }
                }
            }
            call.respond(JsonObject(mapOf()))
            return@get
        }
        post("/poolswaps") {
            val filter = call.receive<SearchFilter>()
            val poolSwaps = searchPoolSwaps(filter, DataType.LIST, 26)
            call.respond(poolSwaps)
        }
        post("/poolliquidity") {
            val filter = call.receive<SearchFilter>()
            val liquidity = searchPoolLiquidity(filter, 26)
            call.respond(liquidity)
        }
        post("/loans") {
            val filter = call.receive<SearchFilter>()
            val liquidity = searchLoans(filter, 26)
            call.respond(liquidity)
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

private fun setFilter(
    uuid: String,
    description: String,
    filter: SearchFilter
): Boolean {
    for (connection in connections) {
        if (connection.uuid == uuid) {
            connection.description = description
            connection.filter = filter
            return true
        }
    }
    return false
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