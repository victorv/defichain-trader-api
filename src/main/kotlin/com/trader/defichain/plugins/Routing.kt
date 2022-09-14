package com.trader.defichain.plugins

import com.trader.defichain.appServerConfig
import com.trader.defichain.db.DB
import com.trader.defichain.dex.*
import com.trader.defichain.http.errorPoolSwapLimitExceeded
import com.trader.defichain.http.errorPoolswapsMissing
import com.trader.defichain.http.poolswapLimit
import com.trader.defichain.http.respond
import com.trader.defichain.telegram.AccountData
import com.trader.defichain.telegram.createAccountId
import com.trader.defichain.telegram.getAccountByChatId
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.http.content.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.cio.*
import io.ktor.util.pipeline.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.io.Writer

suspend fun respondEventStream(call: ApplicationCall, writer: suspend Writer.() -> Unit) {
    call.response.header("X-Accel-Buffering", "no")
    call.response.cacheControl(CacheControl.NoCache(null))
    call.respondTextWriter(contentType = ContentType.Text.EventStream) {
        writer()
    }
}

fun Application.configureRouting() {
    routing {
        static("/") {
            staticRootFolder = File(appServerConfig.webappRoot)

            files(".")

            default("index.html")
        }
        get("/status") {
            call.respond(HttpStatusCode.OK, "ok")
        }
        get("/generate-account-id") {
            call.respondText(ContentType.Application.Json) { Json.encodeToString(createAccountId()) }
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
        post("/telegram-login") {
            val request = call.receive<LoginRequest>()
            val account = getAccountByChatId(request.loginCode)
            if (account == null) {
                call.respond(HttpStatusCode.Unauthorized, "Invalid Login Code")
            } else {
                call.respond(account.getData())
            }
        }
        get("/estimate") {
            val poolSwapParam = call.request.queryParameters["poolswap"]
            if (poolSwapParam == null) {
                call.respond(errorPoolswapsMissing)
                return@get
            }

            val poolSwap = parsePoolSwap("$poolSwapParam desiredResult 1.0")
            poolSwap.checkDesiredResult()
            val testResult = testPoolSwap(poolSwap)
            call.respond(testResult)
        }
        post("/account") {
            val accountData = call.receive<AccountData>()
            val account = getAccountByChatId(accountData.chatID)
            if (account == null) {
                call.respond(HttpStatusCode.Unauthorized, "Account does not exist")
                return@post
            }
            account.update(accountData.poolSwaps)
            call.respond(HttpStatusCode.OK)
        }
        get("/dex") {
            val poolSwaps = parsePoolSwaps() ?: return@get
            val swapResults = poolSwaps.map { testPoolSwap(it) }

            sendEvents(poolSwaps, swapResults)
        }
    }
}

private suspend fun PipelineContext<Unit, ApplicationCall>.parsePoolSwaps(): List<PoolSwap>? {
    val poolSwapsParam = call.request.queryParameters["poolswaps"]
    if (poolSwapsParam == null) {
        call.respond(errorPoolswapsMissing)
        return null
    }

    val requestedPoolSwaps = poolSwapsParam.split(",")
    val size = requestedPoolSwaps.size
    if (size > poolswapLimit) {
        call.respond(errorPoolSwapLimitExceeded)
        return null
    }
    return requestedPoolSwaps.map { parsePoolSwap(it) }
}

private suspend fun PipelineContext<Unit, ApplicationCall>.sendEvents(
    poolSwaps: List<PoolSwap>,
    swapResults: List<SwapResult>,
) {
    val data = Json.encodeToString(swapResults)

    respondEventStream(call) {

        val subscriber = DEXSubscriber(coroutineContext, poolSwaps)
        addSubscriber(subscriber)

        try {
            subscriber.pendingMessages.send("retry: 30000\nevent: poolswap\ndata: $data\n\n")

            while (subscriber.isActive) {

                val message = subscriber.pendingMessages.receive()
                if (subscriber.isActive) {
                    withContext(Dispatchers.IO) {
                        write(message)
                        flush()
                    }
                    subscriber.written++
                }
            }
        } catch (e: ChannelWriteException) {
            call.application.environment.log.info("Closed DEX after writing `${subscriber.written}` messages")
        } finally {
            removeSubscriber(subscriber)
        }
    }
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