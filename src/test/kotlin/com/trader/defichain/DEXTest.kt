package com.trader.defichain

import com.trader.defichain.dex.SwapResult
import com.trader.defichain.test.*
import com.trader.defichain.util.floor
import com.trader.defichain.util.floorPlain
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.opentest4j.AssertionFailedError
import java.math.BigDecimal

private val defaultMaxDelta = BigDecimal("0.00000004")
private val dogeMaxDelta = BigDecimal("0.00000030")
private val `0_2%` = BigDecimal("0.002")
private val `0_003%` = BigDecimal("0.00003")

class DEXTest : UnitTest() {

    companion object {

        private fun asText(poolSwapTests: List<PoolSwapTest>) = poolSwapTests.joinToString(",") {
            "${it.amountFrom}+${it.tokenFrom}+to+${it.tokenTo}+desiredResult+10"
        }

        @JvmStatic
        fun poolSwapParameters(): Iterator<Arguments> {

            val sequence = sequence {
                submitBlock(0)
                val poolSwaps = getBlock().swapResults.iterator()

                val collectedPoolSwapTests = ArrayList<PoolSwapTest>()
                while (poolSwaps.hasNext()) {
                    collectedPoolSwapTests.add(poolSwaps.next())
                    if (collectedPoolSwapTests.size == 10) {
                        yield(
                            Arguments.of(
                                asText(collectedPoolSwapTests),
                                ArrayList(collectedPoolSwapTests)
                            )
                        )
                        collectedPoolSwapTests.clear()
                    }
                }

                if (collectedPoolSwapTests.isNotEmpty()) {
                    yield(
                        Arguments.of(
                            asText(collectedPoolSwapTests),
                            ArrayList(collectedPoolSwapTests)
                        )
                    )
                }
            }
            return sequence.iterator()
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("poolSwapParameters")
    fun test_dex(poolSwaps: String, fullNodeSwapResults: List<PoolSwapTest>) {
        val client = receiveServerSentEvents("dex?poolswaps=$poolSwaps")

        var i = 0
        while (i < 2 || client.hasLines()) {
            val event = client.nextEvent<List<SwapResult>>()
            for ((index, fullNodeSwapResult) in fullNodeSwapResults.withIndex()) {

                // "testpoolswap" often returns swap paths that can never be profitable for a circular swap because they go through the same pool twice.
                // Such paths are not considered by our implementation because it knows they can not be profitable.
                // We skip these cases for now because our result can not be validated against the result of "testpoolswap".
                val pools = fullNodeSwapResult.response!!.pools
                if (fullNodeSwapResult.tokenFrom == fullNodeSwapResult.tokenTo && pools.size == 2 && setOf(pools).size == 1) {
                    continue
                }

                val swapResult = event.data[index]

                assertEquals(fullNodeSwapResult.amountFrom, swapResult.amountFrom)
                assertEquals(fullNodeSwapResult.tokenFrom, swapResult.tokenFrom)
                assertEquals(fullNodeSwapResult.tokenTo, swapResult.tokenTo)


                var maxDelta = defaultMaxDelta
                val fullNodeEstimate = BigDecimal(fullNodeSwapResult.estimate)
                val swapEstimate = BigDecimal(swapResult.estimate)

                val delta = (fullNodeEstimate - swapEstimate).abs()
                if (fullNodeSwapResult.amountFrom <= 0.00001234) {
                    // The error for tiny amounts is significant
                    maxDelta = (fullNodeEstimate * `0_2%`).max(defaultMaxDelta)
                } else if (fullNodeSwapResult.amountFrom <= 0.0005) {
                    // The error for small amounts is negligible
                    maxDelta = (fullNodeEstimate * `0_003%`).max(defaultMaxDelta)
                } else if (fullNodeSwapResult.amountFrom >= 1000.0 && fullNodeSwapResult.tokenTo == "DOGE") {
                    maxDelta = dogeMaxDelta
                }

                if (delta.floor() > maxDelta.floor()) {
                    if (!((fullNodeSwapResult.tokenFrom == "DFI" ||
                                fullNodeSwapResult.tokenFrom == "USDC" ||
                                fullNodeSwapResult.tokenFrom == "USDT") &&
                                fullNodeSwapResult.tokenTo == "DUSD")
                    )
                    /*
                    TODO
                     Remove when bug in "testpoolswap" is resolved where direct USDC or USDT to DUSD swaps return an incorrect estimate.
                     Condition skips large USDT or USDC to DUSD swaps that prompt "testpoolswap" to swap directly from USDC or USDT to DUSD and can not be verified because of the incorrect result returned by "testpoolswap".
                    */ {
                        val description =
                            "${fullNodeSwapResult.amountFrom} ${fullNodeSwapResult.tokenFrom} to ${fullNodeSwapResult.tokenTo} (fullNode=${fullNodeEstimate.floorPlain()}, swap=${swapEstimate.floorPlain()}, delta=${delta.floorPlain()}, maxDelta=${maxDelta.floorPlain()}) ${fullNodeSwapResult.response}"
                        throw AssertionFailedError(description)
                    }
                }
            }

            publishZMQMessage()
            i++
        }
    }

    @Test
    fun exceed_poolswap_limit() {
        val poolSwaps =
            "10.0+DFI+to+BTC+desiredResult+1.0,10.0+DFI+to+DUSD+desiredResult+1.0,10.0+DFI+to+USDT+desiredResult+1.0,10.0+DFI+to+USDC+desiredResult+1.0,10.0+DFI+to+LTC+desiredResult+1.0,10.0+DFI+to+DOGE+desiredResult+1.0,10.0+DFI+to+TSLA+desiredResult+1.0,10.0+DFI+to+GLD+desiredResult+1.0,10.0+BTC+to+DFI+desiredResult+1.0,10.0+BTC+to+DUSD+desiredResult+1.0,10.0+BTC+to+USDT+desiredResult+1.0"

        assertThrows<IllegalStateException>("""{"error":"PoolSwap limit exceeded."}""") {
            receiveServerSentEvents("dex?poolswaps=$poolSwaps")
        }
    }
}