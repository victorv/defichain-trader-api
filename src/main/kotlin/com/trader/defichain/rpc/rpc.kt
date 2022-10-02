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
private val decoder = Json{
    ignoreUnknownKeys = true
}

val limit1000 = JsonObject(
    mapOf(
        "limit" to JsonPrimitive(1000)
    )
)

class RPC {
    companion object {

        suspend inline fun <reified T> tryGet(method: RPCMethod, vararg parameters: JsonElement): RPCResponse<T?> {
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

                val responseBody = response.body<RPCResponse<T?>>()
                check(responseBody.result != null || responseBody.error != null) {
                    "Request failed, invalid response body: ${request}, $responseBody"
                }
                return responseBody

            } catch (e: Throwable) {
                throw RuntimeException("Request failed: $request", e)
            }
        }

        suspend inline fun <reified T> getValue(method: RPCMethod, vararg parameters: JsonElement): T {
            val responseBody = tryGet<T>(method, *parameters)
            check(responseBody.result != null && responseBody.error == null) {
                "Request failed: RPCRequest(${method}, $parameters), $responseBody"
            }
            return responseBody.result
        }
        suspend fun decodeCustomTX(rawTX: String): CustomTX.Record? =
            asCustomTX(tryGet<JsonElement>(RPCMethod.DECODE_CUSTOM_TX, JsonPrimitive(rawTX)).result)

        private fun asCustomTX(result: JsonElement?): CustomTX.Record? {
            if (result is JsonPrimitive || result == null) {
                return null
            }
            return decoder.decodeFromJsonElement<CustomTX.Record>(result)
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