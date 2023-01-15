package com.trader.defichain.db.search

import com.trader.defichain.db.connectionPool
import com.trader.defichain.dex.*
import com.trader.defichain.util.get
import com.trader.defichain.util.prepareStatement
import org.intellij.lang.annotations.Language
import kotlin.math.abs
import kotlin.math.min

private const val oneHour = 120
private const val twelveHours = oneHour * 12
private const val oneDay = oneHour * 24
private const val fiveDays = oneDay * 5
private const val oneMonth = oneDay * 31
private const val twoMonths = oneMonth * 2
private const val threeMonths = oneMonth * 3

@Language("sql")
private val template_poolPairs = """
select 
reserve_a, 
reserve_b, 
pool_pair.token as "pool_id", 
pool_pair.block_height,
in_a,
out_a,
in_b,
out_b,
commission,
block.time as "block_time"
from pool_pair
inner join fee on fee.token = pool_pair.token
 AND fee.block_height = 
 (select coalesce(max(fee.block_height), (select min(block_height) from fee where fee.token = pool_pair.token))
 from fee where fee.token = pool_pair.token AND fee.block_height <= pool_pair.block_height)
inner join block on pool_pair.block_height = block.height
where pool_pair.block_height >= (select max(block.height) - :block_count from block) AND pool_pair.token = ANY(:pool_ids)
order by pool_pair.block_height DESC     
""".trimIndent()

fun getMetrics(poolSwap: AbstractPoolSwap, blockCount: Int): Graph {
    val poolPairUpdates = mutableMapOf<Int, MutableMap<Int, PoolPair>>()
    val uniquePoolIdentifiers = getSwapPaths(poolSwap).flatten().toSet().toTypedArray()

    val blockTimes = mutableMapOf<Int, Long>()
    var minBlockHeight = Integer.MAX_VALUE
    var maxBlockHeight = Integer.MIN_VALUE

    val density = when {
        blockCount <= twelveHours -> 0.0001
        blockCount <= oneDay -> 0.0005
        blockCount <= fiveDays -> 0.001
        blockCount <= oneMonth -> 0.0025
        blockCount <= twoMonths -> 0.005
        blockCount <= threeMonths -> 0.0075
        else -> 0.001
    }

    connectionPool.connection.use { connection ->
        val poolIDArray = connection.createArrayOf("INT", uniquePoolIdentifiers)

        val params = mapOf<String, Any>(
            "pool_ids" to poolIDArray,
            "block_count" to threeMonths.coerceAtMost(blockCount)
        )

        connection.prepareStatement(template_poolPairs, params).use { statement ->

            statement.executeQuery().use { resultSet ->
                while (resultSet.next()) {
                    val poolID = resultSet.getInt("pool_id")

                    val blockHeight = resultSet.getInt("block_height")
                    blockTimes[blockHeight] = resultSet.getLong("block_time")
                    minBlockHeight = min(blockHeight, minBlockHeight)
                    maxBlockHeight = Integer.max(blockHeight, maxBlockHeight)

                    val poolPair = getPool(poolID)
                    val row = PoolPair(
                        symbol = poolPair.symbol,
                        idTokenA = poolPair.idTokenA,
                        idTokenB = poolPair.idTokenB,
                        status = poolPair.status,
                        tradeEnabled = poolPair.tradeEnabled,
                        reserveA = resultSet.get("reserve_a"),
                        reserveB = resultSet.get("reserve_b"),
                        dexFeeInPctTokenA = resultSet.get("in_a"),
                        dexFeeOutPctTokenA = resultSet.get("out_a"),
                        dexFeeInPctTokenB = resultSet.get("in_b"),
                        dexFeeOutPctTokenB = resultSet.get("out_b"),
                        commission = resultSet.getDouble("commission"),
                    )

                    var poolPairsAtHeight = poolPairUpdates[blockHeight]
                    if (poolPairsAtHeight == null) {
                        poolPairsAtHeight = mutableMapOf()
                        poolPairUpdates[blockHeight] = poolPairsAtHeight
                    }
                    poolPairsAtHeight[poolID] = row
                }
            }
        }
    }

    val poolPairs = mutableMapOf<Int, PoolPair>()
    while (minBlockHeight <= maxBlockHeight) {
        val poolPairsAtHeight = poolPairUpdates[minBlockHeight]

        if (poolPairsAtHeight == null) {
            minBlockHeight++
            continue
        }

        for ((poolID, poolPair) in poolPairsAtHeight) {
            poolPairs[poolID] = poolPair
        }

        if (poolPairs.size == uniquePoolIdentifiers.size) {
            break
        }
        minBlockHeight++
    }

    val swapResults = executeSwaps(listOf(poolSwap), poolPairs, true).swapResults.first().breakdown
    val graphBuilders = swapResults.associate { it.path to GraphBuilder(Series(it)) }

    for (height in minBlockHeight..maxBlockHeight) {
        val poolPairsAtHeight = poolPairUpdates[height] ?: continue
        for ((poolID, poolPair) in poolPairsAtHeight) {
            poolPairs[poolID] = poolPair
        }
        val swapResults = executeSwaps(listOf(poolSwap), poolPairs, true).swapResults.first().breakdown

        val time = blockTimes.getValue(height)

        for (swap in swapResults) {
            val estimate = swap.estimate
            val builder = graphBuilders.getValue(swap.path)
            val previousEstimate = builder.previousEstimate
            val previousTime = builder.previousTime

            val uniqueTime = if (time == previousTime) time + 1 else time

            if (abs(previousEstimate - estimate) > previousEstimate * density) {
                builder.series.points.add(Point(estimate, uniqueTime))
                builder.previousEstimate = estimate
                builder.previousTime = uniqueTime
            }
        }
    }

    val swapResult = testPoolSwap(poolSwap)
    for (swap in swapResult.breakdown) {
        val estimate = swap.estimate
        val builder = graphBuilders.getValue(swap.path)
        val previousTime = builder.previousTime

        val time = System.currentTimeMillis() / 1000
        val uniqueTime = if (time == previousTime) time + 1 else time
        builder.series.points.add(Point(estimate, uniqueTime))
    }

    val series = graphBuilders.map { it.value.series }
    return Graph(swapResult, series, density)
}

data class GraphBuilder(
    val series: Series,
    var previousEstimate: Double = 0.0,
    var previousTime: Long = 0,
)

@kotlinx.serialization.Serializable
data class Graph(
    val swap: SwapResult,
    val series: List<Series>,
    val density: Double,
)

@kotlinx.serialization.Serializable
data class Series(
    val swap: PathBreakdown,
    val points: MutableList<Point> = mutableListOf(),
)

@kotlinx.serialization.Serializable
data class Point(
    val value: Double,
    val time: Long,
) {
    override fun equals(o: Any?): Boolean {
        return o is Point && o.value == value
    }

    override fun hashCode(): Int {
        return value.hashCode()
    }
}