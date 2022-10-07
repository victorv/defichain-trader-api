package com.trader.defichain.db

import com.trader.defichain.dex.getOraclePriceForSymbol
import com.trader.defichain.util.floorPlain
import kotlinx.serialization.json.*
import org.intellij.lang.annotations.Language
import org.postgresql.ds.PGSimpleDataSource
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

private val template_tokensSoldRecently = """
select 
sum(amount_from) as sold,
count(*) as tx_count,
token.dc_token_symbol as token_symbol,
max(block_height) as most_recent_block_height
from pool_swap 
inner join token on dc_token_id = token_from 
inner join minted_tx on pool_swap.tx_row_id = minted_tx.tx_row_id 
where block_height >= (select max(block_height) from minted_tx) - 120 
group by token.dc_token_symbol;
""".trimIndent()

private val template_tokensBoughtRecently = """
select 
sum(amount_to) as bought,
count(*) as tx_count,
token.dc_token_symbol as token_symbol,
max(block_height) as most_recent_block_height
from pool_swap 
inner join token on dc_token_id = token_to 
inner join minted_tx on pool_swap.tx_row_id = minted_tx.tx_row_id 
where block_height >= (select max(block_height) from minted_tx) - 120 
group by token.dc_token_symbol;
""".trimIndent()

// TODO for optimal performance some filters should be applied to the join directly and not be part of the where clause
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

    inline fun <reified T> selectAll(view: String): MutableList<T> {
        val results = ArrayList<T>()
        connectionPool.connection.use {

            it.prepareStatement("select * from $view;").use { statement ->

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

                            results.add(Json.decodeFromJsonElement(JsonObject(properties)))
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

    fun tokensSoldRecently() = calcTokenAggregate(template_tokensSoldRecently)
    fun tokensBoughtRecently() = calcTokenAggregate(template_tokensBoughtRecently)
    private fun calcTokenAggregate(sql: String): List<TokenAggregate> {
        val aggregates = ArrayList<TokenAggregate>()
        connectionPool.connection.use {

            it.prepareStatement(sql).use { statement ->
                statement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {

                        val aggregate = resultSet.getDouble(1)
                        val txCount = resultSet.getInt(2)
                        val tokenSymbol = resultSet.getString(3)
                        val mostRecentBlockHeight = resultSet.getLong(4)

                        val oraclePrice = getOraclePriceForSymbol(tokenSymbol)
                        val aggregateUSD = (oraclePrice ?: 0.0) * aggregate

                        aggregates.add(
                            TokenAggregate(
                                aggregate = aggregate,
                                aggregateUSD = aggregateUSD,
                                txCount = txCount,
                                tokenSymbol = tokenSymbol,
                                mostRecentBlockHeight = mostRecentBlockHeight
                            )
                        )
                    }
                }
            }
        }
        return aggregates.sortedByDescending { it.aggregateUSD }
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

        return PoolSwapRow(
            txID = resultSet.getString(1),
            fee = resultSet.getBigDecimal(4).floorPlain(),
            amountFrom = resultSet.getBigDecimal(5).floorPlain(),
            amountTo = resultSet.getBigDecimal(6)?.floorPlain(),
            tokenFrom = resultSet.getString(7),
            tokenTo = resultSet.getString(8),
            maxPrice = resultSet.getBigDecimal(9).floorPlain(),
            from = resultSet.getString(10),
            to = resultSet.getString(11),
            tokenToAlt = resultSet.getString(15),
            id = resultSet.getLong(16),
            block = blockEntry,
            mempool = mempoolEntry
        )
    }

    @kotlinx.serialization.Serializable
    data class TokenAggregate(
        val tokenSymbol: String,
        val aggregate: Double,
        val txCount: Int,
        val aggregateUSD: Double,
        val mostRecentBlockHeight: Long,
    )

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