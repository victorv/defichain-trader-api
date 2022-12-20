package com.trader.defichain.dex

import kotlinx.serialization.Serializable

fun testPoolSwap(poolSwap: AbstractPoolSwap): SwapResult {
    return executeSwaps(listOf(poolSwap), getActivePools(), true).swapResults.first()
}

interface AbstractPoolSwap {
    val amountFrom: Double
    val tokenFrom: String
    val tokenTo: String
    val desiredResult: Double?
}

@Serializable
data class PoolSwap(
    override val amountFrom: Double,
    override val tokenFrom: String,
    override val tokenTo: String,
    override val desiredResult: Double? = null,
    val estimate: Double? = null,
) : AbstractPoolSwap {
    init {
        check(amountFrom > 0 && amountFrom < 999999999) { "Invalid `amount from`: $amountFrom" }
        check(getTokenId(tokenFrom) != null) { "Unable to resolve: $tokenFrom" }
        check(getTokenId(tokenTo) != null) { "Unable to resolve: $tokenTo" }
    }

    fun checkDesiredResult() =
        check(desiredResult != null && desiredResult > 0 && desiredResult < 999999999) { "Invalid `desired result`: $desiredResult" }
}

@Serializable
data class PoolSwapTestResponse(
    val path: String,
    val pools: List<String>,
    val amount: String,
)

@Serializable
data class PathBreakdown(
    val swaps: List<PoolSwapExplained>,
    val estimate: Double,
    val status: Boolean,
    val tradeEnabled: Boolean,
    val overflow: Boolean,
    val price: Double?,
    val premium: Double?
) {
    fun isBad() = overflow || !status || !tradeEnabled
}

@Serializable
data class SwapResult(
    override val amountFrom: Double,
    override val tokenFrom: String,
    override val tokenTo: String,
    override val desiredResult: Double? = null,
    val breakdown: List<PathBreakdown>,
    val estimate: Double,
    val maxPrice: Double,
    val oraclePrice: Double?,
) : AbstractPoolSwap
