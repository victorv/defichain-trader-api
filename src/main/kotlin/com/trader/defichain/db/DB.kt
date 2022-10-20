package com.trader.defichain.db

import com.trader.defichain.dex.*
import com.trader.defichain.util.floorPlain
import kotlinx.serialization.json.*
import org.intellij.lang.annotations.Language
import org.postgresql.ds.PGSimpleDataSource
import java.lang.Integer.max
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.math.abs
import kotlin.math.min

@Language("sql")
private val template_boughtSold = """
with latest_oracle as (
select token, max(block_height) as block_height from oracle_price group by token
),
latest_oracle_price as (
select oracle_price.token, oracle_price.price from oracle_price 
inner join latest_oracle lo on lo.token=oracle_price.token AND lo.block_height=oracle_price.block_height
),
sold as (
select 
sum(amount_from) as sold,	
count(*) as tx_count,
dc_token_id
from pool_swap 
inner join token on dc_token_id = token_from	
inner join minted_tx on pool_swap.tx_row_id = minted_tx.tx_row_id 
where block_height >= (select max(block_height) from minted_tx) - :period
group by token.dc_token_id
),
sold_usd as (
select s.sold as amount, s.tx_count, s.sold * lop.price as amount_usd, token.dc_token_symbol from sold s
inner join latest_oracle_price lop on lop.token = dc_token_id 
inner join token on token.dc_token_id = s.dc_token_id
),
bought as (
select 
sum(amount_to) as bought,
count(*) as tx_count,
dc_token_id
from pool_swap 
inner join token on dc_token_id = token_to 
inner join minted_tx on pool_swap.tx_row_id = minted_tx.tx_row_id 
where block_height >= (select max(block_height) from minted_tx) - :period 
group by token.dc_token_id
),
bought_usd as (
select b.bought as amount, b.tx_count, b.bought * lop.price as amount_usd, token.dc_token_symbol from bought b
inner join latest_oracle_price lop on lop.token = dc_token_id 
inner join token on token.dc_token_id = b.dc_token_id
)
select 
coalesce(s.amount, 0) as sold, 
coalesce(s.amount_usd, 0) as sold_usd, 
coalesce(s.tx_count, 0) as sold_tx_count, 
coalesce(b.amount, 0) as bought, 
coalesce(b.amount_usd, 0) as bought_usd, 
coalesce(b.tx_count, 0) as bought_tx_count, 
coalesce(b.amount - s.amount, 0) net, 
coalesce(b.amount_usd - s.amount_usd, 0) net_usd,
coalesce(b.dc_token_symbol, s.dc_token_symbol) as token_symbol
from sold_usd s full outer join bought_usd b on b.dc_token_symbol=s.dc_token_symbol;
""".trimIndent()

@Language("sql")
private val template_poolPairs = """
select 
reserve_a, 
reserve_b, 
pool_pair.token, 
pool_pair.block_height,
in_a,
out_a,
in_b,
out_b,
commission,
block.time
from pool_pair
inner join fee on fee.token = pool_pair.token
 AND fee.block_height = 
 (select coalesce(max(fee.block_height), (select min(block_height) from fee where fee.token = pool_pair.token))
 from fee where fee.token = pool_pair.token AND fee.block_height <= pool_pair.block_height)
inner join block on pool_pair.block_height = block.height
where pool_pair.block_height >= (select max(block.height) - (2880 * 7) from block) AND pool_pair.token = ANY(?)
order by pool_pair.block_height DESC     
""".trimIndent()

@Language("sql")
private val template_selectPoolSwaps = """
with minted_swap as (
 select 
 pool_swap.tx_row_id,
 block_height,
 txn
 from pool_swap
 inner join minted_tx on minted_tx.tx_row_id = pool_swap.tx_row_id 
 where 
  (? IS NULL or block_height <= ?) AND
  pool_swap.tx_row_id <> ANY(?) AND
  (? IS NULL or token_from = ?) AND
  (? IS NULL or token_to = ?) AND 
  (? IS NULL or ("from" = ? or "to" = ?)) AND
  (? IS NULL or block_height = ?) AND
  (? IS NULL or pool_swap.tx_row_id = ?)
 order by minted_tx.block_height DESC, minted_tx.txn
 limit 26 offset 0
),
mempool_swap as (
 select 
 mempool.tx_row_id,
 block_height,
 -1
 from pool_swap
 inner join mempool on mempool.tx_row_id = pool_swap.tx_row_id
 where
  pool_swap.tx_row_id NOT IN (select tx_row_id from minted_swap) AND
  (? IS NULL or block_height <= ?) AND
  pool_swap.tx_row_id <> ANY(?) AND
  (? IS NULL or token_from = ?) AND
  (? IS NULL or token_to = ?) AND
  (? IS NULL or ("from" = ? or "to" = ?)) AND
  (? IS NULL or block_height = ?) AND
  (? IS NULL or pool_swap.tx_row_id = ?)
 order by mempool.block_height DESC, mempool.txn
 limit 26
),
swaps as (
select * from minted_swap union all select * from mempool_swap order by block_height DESC, txn limit 26
)
select
tx.dc_tx_id as tx_id,
minted_tx.block_height, 
minted_tx.txn, 
tx.fee, 
amount_from, 
amount_to, 
tf.dc_token_symbol as token_from, 
tt.dc_token_symbol as token_to,
max_price,
af.dc_address as "from",
at.dc_address as "to",
mempool.block_height,
mempool.time,
mempool.txn,
tta.dc_token_symbol,
tx.row_id
from pool_swap
inner join swaps on swaps.tx_row_id = pool_swap.tx_row_id
inner join token tf on tf.dc_token_id=token_from 
inner join token tt on tt.dc_token_id=token_to
inner join token tta on tta.dc_token_id=token_to_alt
inner join address af on af.row_id = "from"
inner join address at on at.row_id = "to"
inner join tx on tx.row_id = pool_swap.tx_row_id
left join minted_tx on minted_tx.tx_row_id = pool_swap.tx_row_id 
left join mempool on mempool.tx_row_id = pool_swap.tx_row_id;
""".trimIndent()

private val templates = mapOf(
    "bought_sold" to template_boughtSold
)
val connectionPool = createReadonlyDataSource()

private fun createReadonlyDataSource(): PGSimpleDataSource {
    val connectionPool = PGSimpleDataSource()
    connectionPool.isReadOnly = true
    connectionPool.databaseName = "trader"
    connectionPool.user = "postgres"
    connectionPool.password = "postgres"
    return connectionPool
}

fun insertOrDoNothing(statement: PreparedStatement) {
    val updateCount = statement.executeUpdate()
    check(updateCount == 0 || updateCount == 1)
}

fun upsertReturning(statement: PreparedStatement): Long {
    statement.executeQuery().use { resultSet ->
        check(resultSet.next())

        val rowID = resultSet.getLong(1)
        check(rowID >= 0)

        check(!resultSet.next())
        return rowID
    }
}

object DB {

    fun asJsonElement(value: Any): JsonElement = when (value) {
        null -> JsonNull
        is String -> JsonPrimitive(value)
        is Long -> JsonPrimitive(value)
        is Int -> JsonPrimitive(value)
        is Double -> JsonPrimitive(value)
        is Boolean -> JsonPrimitive(value)
        is BigDecimal -> JsonPrimitive(value)
        else -> throw IllegalArgumentException("Can not convert to JsonElement: $value")
    }

    inline fun <reified T> selectAll(query: String): MutableList<T> {
        val results = ArrayList<T>()
        for(record in selectAllRecords(query)) {
            results.add(Json.decodeFromJsonElement(record))
        }
        return results
    }
    inline fun  selectAllRecords(query: String): MutableList<JsonObject> {
        val results = ArrayList<JsonObject>()
        connectionPool.connection.use {

            it.prepareStatement(query).use { statement ->

                statement.executeQuery().use { resultSet ->

                    val metaData = resultSet.metaData
                    val columnLabels = ArrayList<String>(metaData.columnCount)
                    for (i in 1..metaData.columnCount) {
                        columnLabels.add(resultSet.metaData.getColumnLabel(i))
                    }

                    val isPrimitive = metaData.columnCount == 1

                    while (resultSet.next()) {

                        if (isPrimitive) {
                            results.add(
                                Json.decodeFromJsonElement(
                                    asJsonElement(resultSet.getObject(1))
                                )
                            )
                        } else {
                            val properties = HashMap<String, JsonElement>()
                            for ((i, columnLabel) in columnLabels.withIndex()) {
                                properties[columnLabel] = asJsonElement(
                                    resultSet.getObject(i + 1)
                                )
                            }

                            results.add(JsonObject(properties))
                        }
                    }
                }
            }
        }
        return results
    }

    fun getPoolSwaps(filter: PoolHistoryFilter): List<PoolSwapRow> {
        connectionPool.connection.use { connection ->
            var maxBlockHeight: Long? = null
            var blacklist = arrayOf<Long>(-1)
            var fromTokenID: Int? = null
            var toTokenID: Int? = null
            var addressRowID: Long? = null
            var blockHeight: Long? = null
            var txRowID: Long? = null

            if (filter.fromTokenSymbol != null) {
                fromTokenID = selectTokenID(connection, filter.fromTokenSymbol)
            }

            if (filter.toTokenSymbol != null) {
                toTokenID = selectTokenID(connection, filter.toTokenSymbol)
            }

            val filterString = filter.filterString
            if (filterString != null) {

                // TODO research if address length can be >= 64
                if (filterString.length != 64) {
                    addressRowID = selectAddressRowID(connection, filterString)
                } else {
                    blockHeight = selectBlockHeight(connection, filterString)
                    if (blockHeight == null) {
                        txRowID = selectTransactionRowID(connection, filterString)
                    }
                }
            }

            if (filter.pager != null) {
                maxBlockHeight = filter.pager.maxBlockHeight
                blacklist = filter.pager.blacklist.toTypedArray()
            }
            val blacklistArray = connection.createArrayOf("BIGINT", blacklist)

            val poolSwaps = ArrayList<PoolSwapRow>()
            connection.prepareStatement(template_selectPoolSwaps).use { statement ->
                statement.setObject(1, maxBlockHeight, Types.BIGINT)
                statement.setObject(2, maxBlockHeight, Types.BIGINT)
                statement.setArray(3, blacklistArray)
                statement.setObject(4, fromTokenID, Types.INTEGER)
                statement.setObject(5, fromTokenID, Types.INTEGER)
                statement.setObject(6, toTokenID, Types.INTEGER)
                statement.setObject(7, toTokenID, Types.INTEGER)
                statement.setObject(8, addressRowID, Types.BIGINT)
                statement.setObject(9, addressRowID, Types.BIGINT)
                statement.setObject(10, addressRowID, Types.BIGINT)
                statement.setObject(11, blockHeight, Types.BIGINT)
                statement.setObject(12, blockHeight, Types.BIGINT)
                statement.setObject(13, txRowID, Types.BIGINT)
                statement.setObject(14, txRowID, Types.BIGINT)

                statement.setObject(15, maxBlockHeight, Types.BIGINT)
                statement.setObject(16, maxBlockHeight, Types.BIGINT)
                statement.setArray(17, blacklistArray)
                statement.setObject(18, fromTokenID, Types.INTEGER)
                statement.setObject(19, fromTokenID, Types.INTEGER)
                statement.setObject(20, toTokenID, Types.INTEGER)
                statement.setObject(21, toTokenID, Types.INTEGER)
                statement.setObject(22, addressRowID, Types.BIGINT)
                statement.setObject(23, addressRowID, Types.BIGINT)
                statement.setObject(24, addressRowID, Types.BIGINT)
                statement.setObject(25, blockHeight, Types.BIGINT)
                statement.setObject(26, blockHeight, Types.BIGINT)
                statement.setObject(27, txRowID, Types.BIGINT)
                statement.setObject(28, txRowID, Types.BIGINT)

                statement.executeQuery().use { resultSet ->

                    while (resultSet.next()) {
                        poolSwaps.add(
                            getPoolSwapRow(resultSet)
                        )
                    }
                }
            }

            if (poolSwaps.isEmpty()) {
                return poolSwaps
            }
            return poolSwaps
        }
    }

    private fun selectBlockHeight(
        connection: Connection,
        hash: String,
    ): Long? {
        val sql = "select height from block where hash = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, hash)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return null
                return resultSet.getLong(1)
            }
        }
    }

    private fun selectTransactionRowID(
        connection: Connection,
        txID: String,
    ): Long? {
        val sql = "select row_id from tx where dc_tx_id = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, txID)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return null
                return resultSet.getLong(1)
            }
        }
    }

    private fun selectAddressRowID(
        connection: Connection,
        address: String,
    ): Long? {
        val sql = "select row_id from address where dc_address = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, address)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return null
                return resultSet.getLong(1)
            }
        }
    }

    private fun selectTokenID(
        connection: Connection,
        tokenSymbol: String,
    ): Int? {
        val sql = "select dc_token_id from token where dc_token_symbol = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, tokenSymbol)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return null
                return resultSet.getInt(1)
            }
        }
    }

    fun getMetrics(poolSwap: AbstractPoolSwap): List<List<Double>> {
        val poolPairUpdates = mutableMapOf<Int, MutableMap<Int, PoolPair>>()
        val uniquePoolIdentifiers = getSwapPaths(poolSwap).flatten().toSet().toTypedArray()

        val blockTimes = mutableMapOf<Int, Long>()
        var minBlockHeight = Integer.MAX_VALUE
        var maxBlockHeight = Integer.MIN_VALUE

        connectionPool.connection.use { connection ->
            val poolIDArray = connection.createArrayOf("INT", uniquePoolIdentifiers)

            connection.prepareStatement(template_poolPairs).use { statement ->
                statement.setArray(1, poolIDArray)
                statement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val poolID = resultSet.getInt(3)

                        val blockHeight = resultSet.getInt(4)
                        blockTimes[blockHeight] = resultSet.getLong(10)
                        minBlockHeight = min(blockHeight, minBlockHeight)
                        maxBlockHeight = max(blockHeight, maxBlockHeight)

                        val poolPair = getPool(poolID)
                        val row = PoolPair(
                            symbol = poolPair.symbol,
                            idTokenA = poolPair.idTokenA,
                            idTokenB = poolPair.idTokenB,
                            status = poolPair.status,
                            tradeEnabled = poolPair.tradeEnabled,
                            reserveA = resultSet.getDouble(1),
                            reserveB = resultSet.getDouble(2),
                            dexFeeInPctTokenA = resultSet.getDouble(5),
                            dexFeeOutPctTokenA = resultSet.getDouble(6),
                            dexFeeInPctTokenB = resultSet.getDouble(7),
                            dexFeeOutPctTokenB = resultSet.getDouble(8),
                            commission = resultSet.getDouble(9),
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

    fun stats(templateName: String, period: Int): List<JsonObject> {
        val template = templates.getValue(templateName)
        val sql = template.replace(":period", period.toString())
        return selectAllRecords(sql)
    }

    private fun getPoolSwapRow(resultSet: ResultSet): PoolSwapRow {
        val blockHeight = resultSet.getObject(2)
        val blockEntry = if (blockHeight == null) null else BlockEntry(
            blockHeight = blockHeight as Int,
            txn = resultSet.getInt(3),
        )

        val blockHeightMempool = resultSet.getObject(12)
        val mempoolEntry = if (blockHeightMempool == null) null else MempoolEntry(
            blockHeight = blockHeightMempool as Int,
            time = resultSet.getLong(13),
            txn = resultSet.getInt(14),
        )

        val tokenFrom = resultSet.getString(7)
        val tokenTo = resultSet.getString(8)
        val amountFrom = resultSet.getBigDecimal(5)
        val amountTo = resultSet.getBigDecimal(6)

        val fromOraclePrice = getOraclePriceForSymbol(tokenFrom)
        val fromAmountUSD = (fromOraclePrice ?: 0.0) * amountFrom.toDouble()
        val toOraclePrice = getOraclePriceForSymbol(tokenTo)
        val toAmountUSD = (toOraclePrice ?: 0.0) * (amountTo?.toDouble() ?: 0.0)

        return PoolSwapRow(
            txID = resultSet.getString(1),
            fee = resultSet.getBigDecimal(4).floorPlain(),
            amountFrom = amountFrom.floorPlain(),
            amountTo = amountTo.floorPlain(),
            tokenFrom = tokenFrom,
            tokenTo = tokenTo,
            maxPrice = resultSet.getBigDecimal(9).floorPlain(),
            from = resultSet.getString(10),
            to = resultSet.getString(11),
            block = blockEntry,
            mempool = mempoolEntry,
            tokenToAlt = resultSet.getString(15),
            id = resultSet.getLong(16),
            fromAmountUSD = fromAmountUSD,
            toAmountUSD = toAmountUSD,
            priceImpact = 0.0,
        )
    }

    @kotlinx.serialization.Serializable
    data class BlockEntry(
        val blockHeight: Int,
        val txn: Int,
    )

    @kotlinx.serialization.Serializable
    data class MempoolEntry(
        val blockHeight: Int,
        val txn: Int,
        val time: Long,
    )

    @kotlinx.serialization.Serializable
    data class PoolSwapRow(
        val txID: String,
        val fee: String,
        val amountFrom: String,
        val amountTo: String?,
        val tokenFrom: String,
        val tokenTo: String,
        val maxPrice: String,
        val from: String,
        val to: String,
        val block: BlockEntry?,
        val mempool: MempoolEntry?,
        val tokenToAlt: String,
        val id: Long,
        val fromAmountUSD: Double,
        val toAmountUSD: Double,
        var priceImpact: Double
    )

    @kotlinx.serialization.Serializable
    data class Pager(
        val maxBlockHeight: Long,
        val blacklist: List<Long>,
    )

    @kotlinx.serialization.Serializable
    data class PoolHistoryFilter(
        val fromTokenSymbol: String? = null,
        val toTokenSymbol: String? = null,
        val filterString: String? = null,
        var sort: String? = null,
        val pager: Pager? = null,
    ) {
        companion object {

            val sortOptions = mapOf(
                "fee_asc" to "tx.fee ASC",
                "fee_desc" to "tx.fee DESC",
                "input_amount_asc" to "pool_swap.amount_from ASC",
                "input_amount_desc" to "pool_swap.amount_from DESC",
                "output_amount_asc" to "coalesce(pool_swap.amount_to, 0) ASC",
                "output_amount_desc" to "coalesce(pool_swap.amount_to, 0) DESC",
            )
            val alphaNumeric = "^[a-zA-Z\\d]+$".toRegex()
            val tokenSymbolRegex = "^[a-zA-Z\\d\\./]+$".toRegex()
        }

        private fun checkTokenSymbol(tokenSymbol: String?) {
            if (tokenSymbol == null) return
            check(tokenSymbol.length < 20 && tokenSymbolRegex.matches(tokenSymbol))
        }

        init {
            if (sort != null) {
                sort = sortOptions.getValue(sort!!)
            }

            check(filterString == null || (filterString.length <= 100 && alphaNumeric.matches(filterString)))
            checkTokenSymbol(fromTokenSymbol)
            checkTokenSymbol(toTokenSymbol)
        }
    }

    data class PoolSwap(
        val from: String,
        val to: String,
        val tokenFrom: Int,
        val tokenTo: Int,
        val amountFrom: Double,
        val amountTo: Double?,
        val maxPrice: Double,
    )

    data class MintedTX(
        val txID: String,
        val blockHeight: Int,
        val blockTime: Long,
        val txn: Int,
        val type: String,
    )
}