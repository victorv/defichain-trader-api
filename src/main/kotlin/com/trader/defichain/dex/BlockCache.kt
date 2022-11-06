package com.trader.defichain.dex

import com.trader.defichain.http.gzip
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPC.Companion.listPrices
import com.trader.defichain.rpc.Token
import java.math.BigDecimal
import java.nio.charset.StandardCharsets

private val crypto = setOf("USDT", "USDC", "BTC", "ETH", "DFI", "DOGE", "LTC", "BCH")

private var tokensByID = mapOf<Int, Token>()
private var tokenIdsFromPoolsBySymbol = mapOf<String, Int>()
private var tokensCached = "{}".toByteArray(StandardCharsets.UTF_8)
private var poolPairsSignature = ""
private var pools = mapOf<Int, PoolPair>()
private var allPools = mapOf<Int, PoolPair>()
private var poolPairsCached = "{}".toByteArray(StandardCharsets.UTF_8)
private var swapPaths = mutableMapOf<String, List<List<Int>>>()
fun getCachedPoolPairs() = poolPairsCached

fun getCachedTokens() = tokensCached

fun getSwapPaths(poolSwap: AbstractPoolSwap) =
    swapPaths.getOrDefault("${poolSwap.tokenFrom} to ${poolSwap.tokenTo}", emptyList())

fun getTokens() = tokensByID
fun getTokenSymbol(tokenId: Int): String {
    val token = tokensByID[tokenId] ?: throw IllegalStateException("Unable to find token symbol for token ID $tokenId")
    return token.symbol
}

fun getTokenId(tokenSymbol: String): Int? = tokenIdsFromPoolsBySymbol[tokenSymbol]

private fun cryptoTokenIdentifiers(): Array<Int> {
    val identifiers = mutableSetOf<Int>()
    for (symbol in crypto) {
        val tokenID = getTokenId(symbol)
        if (tokenID != null) {
            identifiers.add(tokenID)
        }
    }
    return identifiers.toTypedArray()
}

private fun stock(includeDUSD: Boolean): Array<Int> {
    val identifiers = mutableSetOf<Int>()
    for (poolPair in pools.values) {
        if (poolPair.idTokenB === 15) {
            val tokenID = poolPair.idTokenA
            val token = tokensByID[tokenID]
            if (token != null && token.symbol != "DFI" && token.symbol != "USDC" && token.symbol != "USDT") {
                identifiers.add(tokenID)
            }
        }
    }
    if (includeDUSD) {
        identifiers.add(15)
    }
    return identifiers.toTypedArray()
}

private fun usdtOrUSDC(): Array<Int> {
    val usdc = getTokenId("USDC")!!
    val usdt = getTokenId("USDT")!!
    return arrayOf(usdc, usdt)
}

fun getTokenIdentifiers(tokenSymbol: String?): Array<Int> {
    if (tokenSymbol == null) {
        return arrayOf()
    }

    val tokenID = tokenIdsFromPoolsBySymbol[tokenSymbol]
    if (tokenID != null) {
        return arrayOf(tokenID)
    }

    return when (tokenSymbol.lowercase()) {
        "crypto" -> cryptoTokenIdentifiers()
        "stock" -> stock(false)
        "dusd_or_stock" -> stock(true)
        "usdt_or_usdc" -> usdtOrUSDC()
        else -> arrayOf()
    }
}

fun getActivePools() = pools

fun getAllPools() = allPools

fun getPool(poolId: Int) = allPools.getValue(poolId)

fun getPoolID(tokenA: Int, tokenB: Int): Int {
    val tokens = setOf(tokenA, tokenB)
    return allPools.entries
        .first { tokens.contains(it.value.idTokenA) && tokens.contains(it.value.idTokenB) }
        .key
}

fun executeSwaps(poolSwaps: List<AbstractPoolSwap>, poolPairs: Map<Int, PoolPair>, forceBestPath: Boolean): DexResult {
    var poolsForAllSwaps = mutableMapOf<Int, PoolPair>()
    val swapResults = ArrayList<SwapResult>()

    for (poolSwap in poolSwaps) {
        var oraclePriceA = getOraclePriceForSymbol(poolSwap.tokenFrom)
        var oraclePriceB = getOraclePriceForSymbol(poolSwap.tokenTo)
        var oraclePrice: Double? = null
        if (oraclePriceA != null && oraclePriceB != null) {
            oraclePrice = oraclePriceB / oraclePriceA
        }

        val allPathsExplained = mutableListOf<PathBreakdown>()

        val paths = getSwapPaths(poolSwap)
        var bestResult = BigDecimal(0)
        var poolsAfterBestSwap = poolsForAllSwaps
        for (path in paths) {
            val poolsForSwap = poolsForAllSwaps.map {
                it.key to it.value.copy(
                    reserveA = it.value.modifiedReserveA.toDouble(),
                    reserveB = it.value.modifiedReserveB.toDouble(),
                )
            }.toMap().toMutableMap()

            val pathExplained = mutableListOf<PoolSwapExplained>()

            var tokenFrom = poolSwap.tokenFrom
            var amountFrom = BigDecimal(poolSwap.amountFrom)
            for (poolId in path) {
                if (!poolsForSwap.containsKey(poolId)) {
                    val pool = poolPairs.getValue(poolId)
                    poolsForSwap[poolId] = pool.copy(
                        reserveA = pool.reserveA,
                        reserveB = pool.reserveB,
                    )
                }
                val pool = poolsForSwap.getValue(poolId)

                val (symbolA, symbolB) = pool.symbol.split("-")
                val (explanation, result) = pool.swap(tokenFrom, amountFrom)
                explanation.poolSymbol = pool.symbol

                pathExplained.add(explanation)
                amountFrom = result
                tokenFrom = if (symbolA == tokenFrom) symbolB else symbolA
            }

            if (amountFrom > bestResult) {
                bestResult = amountFrom
                poolsAfterBestSwap = poolsForSwap
            }

            val estimate = pathExplained.last().amountTo

            val price = if (estimate == 0.0) null else poolSwap.amountFrom / estimate
            val premium = if (oraclePrice == null || price == null) null else 100.0 / oraclePrice * price - 100.0
            allPathsExplained.add(
                PathBreakdown(
                    price = price,
                    premium = premium,
                    overflow = estimate < 0.0,
                    status = pathExplained.all { it.status },
                    tradeEnabled = pathExplained.all { it.tradeEnabled },
                    swaps = pathExplained,
                    estimate = estimate,
                )
            )
        }
        poolsForAllSwaps = poolsAfterBestSwap

        val bestEstimate =
            allPathsExplained.filter { it.status && it.tradeEnabled && !it.overflow }.maxOfOrNull { it.estimate } ?: 0.0
        val desiredResult = poolSwap.desiredResult!!
        val maxPrice = poolSwap.amountFrom / desiredResult
        var pathsBestToWorst =
            allPathsExplained.sortedByDescending { if (it.isBad()) -1.0 * it.estimate else it.estimate }

        val directPath = pathsBestToWorst.find { it.swaps.size == 1 && it.status && it.tradeEnabled }
        if (!forceBestPath && directPath != null) {
            pathsBestToWorst = listOf(directPath)
        }

        val swapResult = SwapResult(
            estimate = bestEstimate,
            desiredResult = poolSwap.desiredResult,
            maxPrice = maxPrice,
            oraclePrice = oraclePrice,
            amountFrom = poolSwap.amountFrom,
            tokenFrom = poolSwap.tokenFrom,
            tokenTo = poolSwap.tokenTo,
            breakdown = pathsBestToWorst,
        )
        swapResults.add(swapResult)
    }

    val poolResults = poolsForAllSwaps.values
        .map {
            val priceBefore = it.getPriceInfo(it.initialReserveA, it.initialReserveB)
            val priceAfter = it.getPriceInfo(it.modifiedReserveA, it.modifiedReserveB)
            val priceChange = it.getPriceChange(priceBefore, priceAfter)

            PoolResult(
                tokenA = getTokenSymbol(it.idTokenA),
                tokenB = getTokenSymbol(it.idTokenB),
                priceBefore = priceBefore,
                priceAfter = priceAfter,
                priceChange = priceChange,
            )
        }

    return DexResult(
        swapResults = swapResults,
        poolResults = poolResults
    )
}

private fun isTradeable(token: Token) = token.destructionHeight == -1 && token.tradeable
suspend fun cachePoolPairs(): Pair<Map<Int, PoolPair>, Map<Int, Double>> {
    allPools = RPC.listPoolPairs()

    val signature = allPools.keys.sorted().joinToString(",")
    val isNewSignature = signature != poolPairsSignature
    if (isNewSignature) {
        cacheAllTokensById()
        poolPairsSignature = signature
    }

    val latestPools = filterPools(allPools)
    if (latestPools == pools) {
        return Pair(emptyMap(), emptyMap())
    }

    if (isNewSignature) {
        cachePoolTokensBySymbol(tokensByID, latestPools.values)
        cacheSwapPaths(latestPools.filter {
            !it.value.symbol.contains("BURN")
        }, tokenIdsFromPoolsBySymbol)
    }

    pools = latestPools
    poolPairsCached = gzip(pools)

    val oraclePrices = assignOraclePrices()
    return Pair(pools, oraclePrices)
}

fun getOraclePrice(tokenID: Int): Double? {
    val token = tokensByID[tokenID] ?: return null
    return token.oraclePrice
}

fun getOraclePriceForSymbol(tokenSymbol: String): Double? {
    val tokenID = tokenIdsFromPoolsBySymbol[tokenSymbol] ?: return null
    return getOraclePrice(tokenID)
}

private suspend fun assignOraclePrices(): Map<Int, Double> {
    val validOraclePrices = mutableMapOf<Int, Double>()

    tokensByID.forEach { it.value.oraclePrice = null }
    val oraclePrices = listPrices().filter { it.ok.content == "true" && it.currency == "USD" }
    for (oraclePrice in oraclePrices) {
        val tokenId = tokenIdsFromPoolsBySymbol[oraclePrice.token] ?: continue
        val token = tokensByID[tokenId] ?: continue
        val oraclePrice = oraclePrice.price ?: continue
        if (oraclePrice > 0.0) {
            token.oraclePrice = oraclePrice
            validOraclePrices[tokenId] = oraclePrice
        }
    }

    val dusdToken = tokensByID[15] ?: return validOraclePrices
    validOraclePrices[15] = 1.0
    dusdToken.oraclePrice = 1.0
    return validOraclePrices
}

private fun filterPools(allPoolPairs: Map<Int, PoolPair>): Map<Int, PoolPair> {
    val latestPoolPairs = allPoolPairs.filter {
        val poolPair = it.value

        val poolPairToken = tokensByID.getValue(it.key)
        if (!isTradeable(poolPairToken)) {
            check(poolPairToken.symbol == poolPair.symbol)
            return@filter false
        }

        val (tokenASymbol, tokenBSymbol) = poolPair.symbol.split("-")

        val tokenA = tokensByID.getValue(poolPair.idTokenA)
        if (!isTradeable(tokenA)) {
            check(tokenA.symbol == tokenASymbol)
            return@filter false
        }

        val tokenB = tokensByID.getValue(poolPair.idTokenB)
        if (!isTradeable(tokenB)) {
            check(tokenB.symbol == tokenBSymbol)
            return@filter false
        }
        true
    }
    return latestPoolPairs
}

private fun cacheSwapPaths(pools: Map<Int, PoolPair>, tokenIdsFromPoolsBySymbol: Map<String, Int>) {
    val poolTree = PoolTree()
    val tokenIds = mutableSetOf<Int>()
    for ((poolId, pool) in pools) {
        tokenIds.add(pool.idTokenA)
        tokenIds.add(pool.idTokenB)
        poolTree.addPool(poolId, pool)
    }

    for (tokenFromSymbol in tokenIdsFromPoolsBySymbol.keys) {
        for (tokenToSymbol in tokenIdsFromPoolsBySymbol.keys) {
            val paths = poolTree.getSwapPaths(tokenFromSymbol, tokenToSymbol)
            swapPaths["$tokenFromSymbol to $tokenToSymbol"] = paths
        }
    }
}

private fun cachePoolTokensBySymbol(allTokens: Map<Int, Token>, pools: Collection<PoolPair>) {
    val tokensFromPoolsById = mutableMapOf<Int, Token>()
    for (pool in pools) {
        tokensFromPoolsById[pool.idTokenA] = allTokens.getValue(pool.idTokenA)
        tokensFromPoolsById[pool.idTokenB] = allTokens.getValue(pool.idTokenB)
    }

    val tokenIdsBySymbol = mutableMapOf<String, Int>()
    for ((tokenId, token) in tokensFromPoolsById) {
        check(!tokenIdsBySymbol.containsKey(token.symbol)) {
            "Duplicate symbol: ${token.symbol}"
        }
        tokenIdsBySymbol[token.symbol] = tokenId
    }
    tokenIdsFromPoolsBySymbol = tokenIdsBySymbol
}

private suspend fun cacheAllTokensById() {
    tokensByID = RPC.listTokens()
    tokensCached = gzip(tokensByID)
}

@kotlinx.serialization.Serializable
data class PriceInfo(
    val forwardPrice: Double,
    val backwardPrice: Double,
) {
    override fun toString() =
        "PriceInfo(forward=${"%.8f".format(forwardPrice)}, backward=${"%.8f".format(backwardPrice)})"
}

@kotlinx.serialization.Serializable
data class PoolSwapExplained(
    val commissionPct: Double?,
    val commission: Double?,
    val inFeePct: Double?,
    val inFee: Double?,
    val outFeePct: Double?,
    val outFee: Double?,
    val tokenFrom: String,
    val tokenTo: String,
    val amountFrom: Double,
    val amountTo: Double,
    val priceBefore: PriceInfo,
    val priceAfter: PriceInfo,
    val priceChange: PriceInfo,
    val status: Boolean,
    val tradeEnabled: Boolean,
    val overflow: Boolean,
    var poolSymbol: String = "Undefined",
)

@kotlinx.serialization.Serializable
data class PoolResult(
    val tokenA: String,
    val tokenB: String,
    val priceBefore: PriceInfo,
    val priceAfter: PriceInfo,
    val priceChange: PriceInfo,
)

@kotlinx.serialization.Serializable
data class DexResult(
    val swapResults: List<SwapResult>,
    val poolResults: List<PoolResult>
)