package com.trader.defichain.rpc

import com.trader.defichain.config.rpcConfig
import com.trader.defichain.dex.PoolPair
import com.trader.defichain.plugins.getHttpClientEngine
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

const val dummyAddress = "dLXs788fWMpGoar1WzDnLoNCNYyZVPPozv"

private const val noSuchTXError = "No such mempool or wallet transaction. Use -txindex or provide a block hash."
private const val notACustomTXError = "Not a custom transaction"
private const val txDecodeFailed = "TX decode failed"
private const val txNotInMempool = "Transaction not in mempool"

val withIgnoreUnknownKeys = Json { ignoreUnknownKeys = true }
val limit1000 = JsonObject(
    mapOf(
        "limit" to JsonPrimitive(1000)
    )
)

class RPC {
    companion object {

        val notErrors = setOf(noSuchTXError, notACustomTXError, txDecodeFailed, txNotInMempool)
        suspend inline fun <reified T> getValue(method: RPCMethod, vararg parameters: JsonElement): T {
            val request = RPCRequest(
                jsonrpc = "1.0",
                method = method.id,
                params = JsonArray(parameters.toList())
            )
            try {
                val response = rpcClient.post("http://${rpcConfig.host}:${rpcConfig.port}") {
                    contentType(ContentType.Application.Json)
                    setBody(request)
                }

                val responseBody: RPCResponse<T?> = response.body()
                if (notErrors.contains(responseBody.error?.message)) {
                    return JsonNull as T
                }

                val result = responseBody.result
                check(result != null) {
                    "Request failed: $request, $responseBody"
                }
                return result
            } catch (e: Throwable) {
                throw RuntimeException("Request failed: $request", e)
            }
        }

        suspend fun getMempoolEntry(txID: String): MempoolEntry? {
            val mempoolEntry = getValue<JsonElement>(RPCMethod.GET_MEMPOOL_ENTRY, JsonPrimitive(txID))
            if (mempoolEntry is JsonPrimitive) {
                return null
            }
            return withIgnoreUnknownKeys.decodeFromJsonElement(mempoolEntry)
        }

        suspend fun getCustomTX(txID: String): CustomTX? {
            val customTX = getValue<JsonElement>(RPCMethod.GET_CUSTOM_TX, JsonPrimitive(txID))
            return asCustomTX(customTX)
        }

        suspend fun decodeCustomTX(rawTX: String): CustomTX? {
            val customTX = getValue<JsonElement>(RPCMethod.DECODE_CUSTOM_TX, JsonPrimitive(rawTX))
            return asCustomTX(customTX)
        }

        private fun asCustomTX(customTX: JsonElement): CustomTX? {
            if (customTX is JsonPrimitive) { // result is an error message string or raw TX could not be decoded
                return null
            }
            return withIgnoreUnknownKeys.decodeFromJsonElement(customTX)
        }

        suspend fun listPoolPairs(): Map<String, PoolPair> = getValue(
            RPCMethod.LIST_POOL_PAIRS,
            limit1000,
        )


        suspend fun listTokens(): Map<String, Token> = getValue(
            RPCMethod.LIST_TOKENS,
            limit1000,
        )

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

@Serializable
data class RPCRequest(
    val jsonrpc: String,
    val method: String,
    val params: JsonArray
)

@Serializable
data class Error(val code: Int, val message: String)

@Serializable
data class RPCResponse<T>(val result: T?, val error: Error?, val id: String?)