package com.trader.defichain.db.search

import com.trader.defichain.db.connectionPool
import com.trader.defichain.dex.*
import com.trader.defichain.util.get
import com.trader.defichain.util.prepareStatement
import org.intellij.lang.annotations.Language
import kotlin.math.abs
import kotlin.math.max
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
where pool_pair.token = ANY(:pool_ids)
order by pool_pair.block_height DESC     
""".trimIndent()

fun getMetrics(poolSwap: AbstractPoolSwap, candleTime: Long, path: Int): List<List<Double>> {

    val poolIdentifiers = getSwapPaths(poolSwap).first { it.hashCode() == path }.toTypedArray()
    val byTime = HashMap<Long, CandleStick>()

    connectionPool.connection.use { connection ->
        val poolIDArray = connection.createArrayOf("INT", poolIdentifiers)

        val params = mapOf<String, Any>(
            "pool_ids" to poolIDArray,
        )

        val poolPairs = mutableMapOf<Int, PoolPair>()
        poolPairs.putAll(getAllPools())

        connection.prepareStatement(template_poolPairs, params).use { statement ->

            statement.executeQuery().use { resultSet ->
                while (resultSet.next() && byTime.size < 100) {
                    val poolID = resultSet.getInt("pool_id")

                    val livePoolPair = getPool(poolID)
                    val newPoolPair = PoolPair(
                        symbol = livePoolPair.symbol,
                        idTokenA = livePoolPair.idTokenA,
                        idTokenB = livePoolPair.idTokenB,
                        status = livePoolPair.status,
                        tradeEnabled = livePoolPair.tradeEnabled,
                        reserveA = resultSet.get("reserve_a"),
                        reserveB = resultSet.get("reserve_b"),
                        dexFeeInPctTokenA = resultSet.get("in_a"),
                        dexFeeOutPctTokenA = resultSet.get("out_a"),
                        dexFeeInPctTokenB = resultSet.get("in_b"),
                        dexFeeOutPctTokenB = resultSet.get("out_b"),
                        commission = resultSet.getDouble("commission"),
                    )

                    val currentPoolPair = poolPairs.getValue(poolID)
                    if (abs(currentPoolPair.reserveA - newPoolPair.reserveA) < currentPoolPair.reserveA * 0.0005) {
                        continue
                    }

                    val time = resultSet.getLong("block_time") * 1000
                    val roundedTime = time - (time % candleTime)
                    poolPairs[poolID] = newPoolPair

                    val estimate = executeSwaps(
                        listOf(poolSwap),
                        poolPairs,
                        true
                    ).swapResults
                        .first()
                        .breakdown
                        .first { it.path == path }
                        .estimate

                    val current = byTime[roundedTime]
                    if (current == null) {
                        byTime[roundedTime] = CandleStick(
                            open = estimate,
                            low = estimate,
                            close = estimate,
                            high = estimate,
                            minTime = time,
                            maxTime = time,
                            time = roundedTime
                        )
                    } else {
                        val o = if (time < current.minTime) estimate else current.open
                        val c = if (time > current.maxTime) estimate else current.close
                        val l = min(current.low, estimate)
                        val h = max(current.high, estimate)
                        byTime[roundedTime] = CandleStick(
                            open = o,
                            close = c,
                            high = h,
                            low = l,
                            minTime = min(time, current.minTime),
                            maxTime = max(time, current.maxTime),
                            time = roundedTime
                        )
                    }
                }
            }
        }
    }

    val candles = ArrayList<List<Double>>(byTime.size)
    var prevCandle: CandleStick? = null
    for (candle in byTime.values.sortedBy { it.time }) {
        val open = prevCandle?.close ?: candle.open
        candles.add(listOf(open, candle.close, candle.low, candle.high, (candle.time / 1000).toDouble()))
        prevCandle = candle
    }
    return candles
}

@kotlinx.serialization.Serializable
data class CandleStick(
    val open: Double,
    val close: Double,
    val high: Double,
    val low: Double,
    val time: Long,
    val maxTime: Long,
    val minTime: Long,
)