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

        suspend inline fun <reified T> tryGet(method: RPCMethod, vararg parameters: JsonElement): RPCResponse<T?> {
            val request = RPCRequest(
                jsonrpc = "1.0",
                method = method.id,
                params = JsonArray(parameters.toList())
            )
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
            val response = rpcClient.post("http://${rpcConfig.host}:${rpcConfig.port}") {
                contentType(ContentType.Application.Json)
                setBody(request)
            }

            val responseBody = response.body<RPCResponse<T?>>()
            check(responseBody.result != null || responseBody.error != null) {
                "Request failed, invalid response body: ${request}, $responseBody"
            }
            return responseBody
        }

        suspend inline fun <reified T> getValue(method: RPCMethod, vararg parameters: JsonElement): T {
            val responseBody = tryGet<T>(method, *parameters)
            check(responseBody.result != null && responseBody.error == null) {
                "Request failed: RPCRequest(${method}, $parameters), $responseBody"
            }
            return responseBody.result
        }

        suspend fun decodeCustomTX(rawTX: String): CustomTX.Record? {
            val ia = rawTX.indexOf("6a")
            val ib = rawTX.indexOf("446654786d")
            if (ia > 0 && ib > 0 && ia < ib) {
                return null
            }
            return asCustomTX(tryGet<JsonElement>(RPCMethod.DECODE_CUSTOM_TX, JsonPrimitive(rawTX)).result)
        }

        suspend fun getMasterNodeTX(txID: String?): MasterNodeTX {
            if (txID == null) {
                return MasterNodeTX(
                    TX(
                        txID = "placeholdertx",
                        vin = listOf(),
                        vout = listOf(),
                        vsize = 100,
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

        suspend fun getAccount(address: String): Map<String, Double> = getValue<List<String>>(
            RPCMethod.GET_ACCOUNT,
            JsonPrimitive(address),
            limit1000,
        ).associate {
            val (amount, token) = it.split("@")
            token to amount.toDouble()
        }

        suspend fun listPrices(): List<OraclePrice> = getValue(
            RPCMethod.LIST_PRICES,
            limit1000,
        )

        suspend fun listAuctions(): List<RPCAuction> = getValue(
            RPCMethod.LIST_AUCTIONS,
            limit1000,
        )

        suspend fun getTokenBalances(): Map<String, Double> = getValue(
            RPCMethod.GET_TOKEN_BALANCES,
            JsonObject(mapOf()),
            JsonPrimitive(true),
            JsonPrimitive(true)
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