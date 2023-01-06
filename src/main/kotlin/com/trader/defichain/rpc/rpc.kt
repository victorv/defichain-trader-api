package com.trader.defichain.rpc

import com.trader.defichain.config.rpcConfig
import com.trader.defichain.dex.PoolPair
import com.trader.defichain.indexer.calculateFee
import com.trader.defichain.plugins.getHttpClientEngine
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.delay
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import java.io.IOException
import java.math.BigDecimal

const val dummyAddress = "dLXs788fWMpGoar1WzDnLoNCNYyZVPPozv"

private val masterNodeTransactions = mutableMapOf<String, MasterNodeTX>()

private val decoder = Json {
    ignoreUnknownKeys = true
}

val limit1000 = JsonObject(
    mapOf(
        "limit" to JsonPrimitive(1000)
    )
)

class RPC {
    companion object {

        suspend inline fun <reified T> tryGet(request: RPCRequest): RPCResponse<T?> {
            try {
                return doRequest<T>(request)
            } catch (e: Throwable) {
                if (e is IOException) {
                    return retryRequest<T>(request)
                }
                throw RuntimeException("Request failed: $request", e)
            }
        }

        suspend inline fun <reified T> retryRequest(request: RPCRequest): RPCResponse<T?> {
            delay(3000)
            try {
                return doRequest<T>(request)
            } catch (e: Throwable) {
                throw RuntimeException("Request failed: $request", e)
            }
        }

        suspend inline fun <reified T> doRequest(request: RPCRequest): RPCResponse<T?> {
            val url = if (request.instance == RPCInstance.LIVE) "http://${rpcConfig.host}:${rpcConfig.port}"
            else "http://${rpcConfig.host}:18554"

            val response = rpcClient.post(url) {
                contentType(ContentType.Application.Json)
                setBody(request)
            }

            val responseBody = response.body<RPCResponse<T?>>()
            if (request.noResponse) return responseBody

            check(responseBody.result != null || responseBody.error != null) {
                "Request failed, invalid response body: ${request}, $responseBody"
            }
            return responseBody
        }

        suspend inline fun <reified T> getValue(request: RPCRequest): T {
            val responseBody = tryGet<T>(request)
            check( responseBody.result != null && responseBody.error == null) {
                "Request failed: $request, $responseBody"
            }
            return responseBody.result
        }

        suspend inline fun <reified T> getValue(method: RPCMethod, vararg parameters: JsonElement): T {
            return getValue(
                RPCRequest(
                    jsonrpc = "1.0",
                    method = method.id,
                    params = JsonArray(parameters.toList())
                )
            )
        }

        suspend fun decodeCustomTX(rawTX: String): CustomTX.Record? {
            val ia = rawTX.indexOf("6a")
            val ib = rawTX.indexOf("446654786d")
            if (ia > 0 && ib > 0 && ia < ib) {
                return null
            }

            val request = RPCRequest(
                jsonrpc = "1.0",
                method = RPCMethod.DECODE_CUSTOM_TX.id,
                params = JsonArray(listOf(JsonPrimitive(rawTX)))
            )
            return asCustomTX(tryGet<JsonElement>(request).result)
        }

        suspend fun getMasterNodeTX(txID: String?): MasterNodeTX {
            if (txID == null) {
                return MasterNodeTX(
                    TX(
                        txID = "placeholdertx",
                        vin = listOf(),
                        vout = listOf(),
                        size = 100,
                    ), BigDecimal(1.0)
                )
            }

            if (masterNodeTransactions.containsKey(txID)) return masterNodeTransactions.getValue(txID)

            val tx = getValue<TX>(RPCMethod.GET_RAW_TRANSACTION, JsonPrimitive(txID), JsonPrimitive(true))
            val fee = calculateFee(tx, mapOf())
            val masterNodeTX = MasterNodeTX(tx, fee)
            masterNodeTransactions[tx.txID] = masterNodeTX
            return masterNodeTX
        }

        private fun asCustomTX(result: JsonElement?): CustomTX.Record? {
            if (result is JsonPrimitive || result == null) {
                return null
            }
            return decoder.decodeFromJsonElement<CustomTX.Record>(result)
        }

        suspend fun listPoolPairs(): Map<Int, PoolPair> = getValue<Map<String, PoolPair>>(
            RPCMethod.LIST_POOL_PAIRS,
            limit1000,
        ).entries.associate { it.key.toInt() to it.value }


        suspend fun listTokens(): Map<Int, Token> = getValue<Map<String, Token>>(
            RPCMethod.LIST_TOKENS,
            limit1000,
        ).entries.associate { it.key.toInt() to it.value }

        suspend fun listPrices(): List<OraclePrice> = getValue(
            RPCMethod.LIST_PRICES,
            limit1000,
        )
    }
}

val rpcClient = HttpClient(getHttpClientEngine()) {
    expectSuccess = false

    install(ContentNegotiation) {
        json(Json {
            ignoreUnknownKeys = true
        })
    }

    install(Auth) {
        basic {
            credentials {
                BasicAuthCredentials(
                    username = rpcConfig.user,
                    password = rpcConfig.password
                )
            }
        }
    }
}

enum class RPCInstance {
    STANDBY,
    LIVE
}

@Serializable
data class RPCRequest(
    val jsonrpc: String,
    val method: String,
    val params: JsonArray,
    val instance: RPCInstance = RPCInstance.LIVE,
    val noResponse: Boolean = false,
)

@Serializable
data class Error(val code: Int, val message: String)

@Serializable
data class RPCResponse<T>(val result: T?, val error: Error?, val id: String?)