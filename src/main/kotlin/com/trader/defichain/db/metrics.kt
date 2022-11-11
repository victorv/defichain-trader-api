package com.trader.defichain.db

import com.trader.defichain.dex.*
import com.trader.defichain.util.get
import com.trader.defichain.util.prepareStatement
import org.intellij.lang.annotations.Language
import kotlin.math.abs
import kotlin.math.min

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
where pool_pair.block_height >= (select max(block.height) - (2880 * 7) from block) AND pool_pair.token = ANY(:pool_ids)
order by pool_pair.block_height DESC     
""".trimIndent()

fun getMetrics(poolSwap: AbstractPoolSwap): List<List<Double>> {
    val poolPairUpdates = mutableMapOf<Int, MutableMap<Int, PoolPair>>()
    val uniquePoolIdentifiers = getSwapPaths(poolSwap).flatten().toSet().toTypedArray()

    val blockTimes = mutableMapOf<Int, Long>()
    var minBlockHeight = Integer.MAX_VALUE
    var maxBlockHeight = Integer.MIN_VALUE

    connectionPool.connection.use { connection ->
        val poolIDArray = connection.createArrayOf("INT", uniquePoolIdentifiers)

        val params = mapOf<String, Any>(
            "pool_ids" to poolIDArray
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

    val metrics = ArrayList<List<Double>>()
    var previousEstimate = 0.0
    for (height in minBlockHeight..maxBlockHeight) {
        val poolPairsAtHeight = poolPairUpdates[height] ?: continue
        for ((poolID, poolPair) in poolPairsAtHeight) {
            poolPairs[poolID] = poolPair
        }

        val estimate = executeSwaps(listOf(poolSwap), poolPairs, true).swapResults.first().estimate
        if (abs(estimate - previousEstimate) < estimate * 0.0001) continue

        metrics.add(
            listOf(
                height.toDouble(),
                estimate,
                blockTimes.getValue(height).toDouble()
            )
        )
        previousEstimate = estimate
    }

    return metrics
}