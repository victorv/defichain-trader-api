package com.trader.defichain.test

import com.trader.defichain.dex.AbstractPoolSwap
import com.trader.defichain.dex.PoolPair
import com.trader.defichain.dex.PoolSwapTestResponse
import com.trader.defichain.loadAppServerConfig
import com.trader.defichain.rpc.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.encodeToJsonElement
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kotlin.io.path.absolutePathString

private val capturedBlocks = ArrayList<CapturedBlock>()
private val swapResults = ArrayList<PoolSwapTest>()

suspend fun checkBlockCountIs(expectedBlockCount: Int) {
    val currentBlockCount = RPC.getValue<Int>(RPCMethod.GET_BLOCK_COUNT)
    check(expectedBlockCount == currentBlockCount) {
        "Block has changed, expected: $expectedBlockCount, got: $currentBlockCount"
    }
}

suspend fun main() {
    val testResourcesDir = getTestResourcesDirectory()
    loadAppServerConfig(testResourcesDir.resolve("config.json").absolutePathString())

    val blockCount = RPC.getValue<Int>(RPCMethod.GET_BLOCK_COUNT)
    val tokens = RPC.listTokens()
        .entries.associate { it.key.toString() to it.value }
    val poolPairs = RPC.listPoolPairs()
        .entries.associate { it.key.toString() to it.value }
    val prices = RPC.listPrices()
    checkBlockCountIs(blockCount)

    capturePoolSwapTests()
    checkBlockCountIs(blockCount)

    capturedBlocks.add(
        CapturedBlock(
            height = blockCount,
            tokens = tokens,
            poolPairs = poolPairs,
            prices = prices,
            swapResults = swapResults,
        )
    )

    val blocksFile = testResourcesDir.resolve("blocks.json")
    val blocksJSON = Json.encodeToString(capturedBlocks)
    Files.write(blocksFile, blocksJSON.toByteArray(StandardCharsets.UTF_8))
}

private suspend fun testPoolSwap(tokenFrom: String, tokenTo: String, amountFrom: Double): PoolSwapTest {
    val poolSwapTest = PoolSwapTest(
        amountFrom = amountFrom,
        tokenFrom = tokenFrom,
        tokenTo = tokenTo,
        from = dummyAddress,
        to = dummyAddress,
    )
    val encodedPoolSwapTest = Json.encodeToJsonElement(poolSwapTest)
    val autoResponse = RPC.getValue<PoolSwapTestResponse>(
        RPCMethod.TEST_POOL_SWAP,
        encodedPoolSwapTest,
        JsonPrimitive("auto"),
        JsonPrimitive(true),
    )

    var estimate = autoResponse.amount.split("@").first().toDouble()
    var response = autoResponse

    if (autoResponse.path == "direct") {
        val compositeResponse = RPC.getValue<PoolSwapTestResponse>(
            RPCMethod.TEST_POOL_SWAP,
            encodedPoolSwapTest,
            JsonPrimitive("composite"),
            JsonPrimitive(true),
        )
        val compositeEstimate = compositeResponse.amount.split("@").first().toDouble()

        if (compositeEstimate > estimate) {
            estimate = compositeEstimate
            response = compositeResponse
        }
    }

    poolSwapTest.estimate = estimate
    poolSwapTest.response = response
    return poolSwapTest
}

private suspend fun capturePoolSwapTests() {
    val tokens = setOf("USDT", "USDC", "DFI", "BTC", "DOGE", "LTC", "ETH", "BCH", "DUSD", "TSLA", "AMZN")
    val amounts = listOf(9000000.0, 1000.0, 0.0005, 0.00001234)

    for (amount in amounts) {
        for (fromTokenSymbol in tokens) {
            for (toTokenSymbol in tokens) {
                if (fromTokenSymbol == "DFI" && toTokenSymbol == "DUSD") {
                    continue
                }

                val result = testPoolSwap(fromTokenSymbol, toTokenSymbol, amount)
                swapResults.add(result)
            }
        }
    }
}

@Serializable
data class CapturedBlock(
    val swapResults: List<PoolSwapTest>,
    val tokens: Map<String, Token>,
    val poolPairs: Map<String, PoolPair>,
    val prices: List<OraclePrice>,
    val height: Int
) {
    val tokensJson = Json.encodeToString(
        RPCResponse(
            result = tokens,
            error = null,
            id = null,
        )
    )

    val poolPairsJson = Json.encodeToString(
        RPCResponse(
            result = poolPairs,
            error = null,
            id = null,
        )
    )

    val pricesJson = Json.encodeToString(
        RPCResponse(
            result = prices,
            error = null,
            id = null,
        )
    )

    val heightJson = Json.encodeToString(
        RPCResponse(
            result = height,
            error = null,
            id = null,
        )
    )
}

@Serializable
data class PoolSwapTest(
    override val amountFrom: Double,
    override val tokenFrom: String,
    override val tokenTo: String,
    override val desiredResult: Double? = null,
    val from: String,
    val to: String,
    var estimate: Double = 0.0,
    var response: PoolSwapTestResponse? = null,
) : AbstractPoolSwap