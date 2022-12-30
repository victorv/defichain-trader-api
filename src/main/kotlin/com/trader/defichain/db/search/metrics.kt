package com.trader.defichain.db.search

import com.trader.defichain.db.connectionPool
import com.trader.defichain.dex.*
import com.trader.defichain.util.get
import com.trader.defichain.util.prepareStatement
import org.intellij.lang.annotations.Language
import java.time.*
import kotlin.math.max
import kotlin.math.min

private val UTC = ZoneId.of("UTC")

private const val oneHour = 120
private const val oneDay = oneHour * 24
private const val fiveDays = oneDay * 5
private const val threeMonths = oneDay * 31 * 3

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

fun getMetrics(poolSwap: AbstractPoolSwap, blockCount: Int): List<List<Double>> {
    val poolPairUpdates = mutableMapOf<Int, MutableMap<Int, PoolPair>>()
    val uniquePoolIdentifiers = getSwapPaths(poolSwap).flatten().toSet().toTypedArray()

    val blockTimes = mutableMapOf<Int, Long>()
    var minBlockHeight = Integer.MAX_VALUE
    var maxBlockHeight = Integer.MIN_VALUE

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

    val byTime = HashMap<Long, CandleStick>()
    for (height in minBlockHeight..maxBlockHeight) {


        val poolPairsAtHeight = poolPairUpdates[height] ?: continue
        for ((poolID, poolPair) in poolPairsAtHeight) {
            poolPairs[poolID] = poolPair
        }

        val estimate = executeSwaps(listOf(poolSwap), poolPairs, true).swapResults.first().estimate

        val time = blockTimes.getValue(height)
        val roundedTime = roundToFitTimeline(time, blockCount)
        val current = byTime[roundedTime]
        if (current == null) {
            byTime[roundedTime] = CandleStick(
                o = estimate,
                l = estimate,
                c = estimate,
                h = estimate,
                blockHeight = height,
                minTime = time,
                maxTime = time,
                roundedTime = roundedTime
            )
        } else {
            val o = if (time < current.minTime) estimate else current.o
            val c = if (time > current.maxTime) estimate else current.c
            val l = min(current.l, estimate)
            val h = max(current.h, estimate)
            byTime[roundedTime] = CandleStick(
                o = o,
                c = c,
                h = h,
                l = l,
                blockHeight = height,
                minTime = min(time, current.minTime),
                maxTime = max(time, current.maxTime),
                roundedTime = roundedTime
            )
        }
    }
    return byTime.values.sortedBy { it.roundedTime }.map {
        listOf(it.o, it.c, it.l, it.h, it.roundedTime.toDouble(), it.blockHeight.toDouble())
    }
}

private fun roundToFitTimeline(time: Long, blockCount: Int): Long {
    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time * 1000), UTC)
    return when {
        blockCount <= oneHour -> dateTime
        blockCount <= fiveDays -> LocalDateTime.of(
            dateTime.toLocalDate(),
            LocalTime.of(dateTime.hour, 0)
        )
        else -> LocalDateTime.of(
            dateTime.toLocalDate(),
            LocalTime.of(0, 0)
        )
    }.toInstant(ZoneOffset.UTC).toEpochMilli()
}

data class CandleStick(
    val o: Double,
    val c: Double,
    val h: Double,
    val l: Double,
    val blockHeight: Int,
    val roundedTime: Long,
    val maxTime: Long,
    val minTime: Long,
)