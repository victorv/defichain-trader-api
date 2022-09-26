package com.trader.defichain.db

import com.trader.defichain.dex.getOraclePriceForSymbol
import com.trader.defichain.util.floorPlain
import kotlinx.serialization.json.*
import org.postgresql.ds.PGSimpleDataSource
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

private val logger = LoggerFactory.getLogger("DB")

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

private val template_selectPoolSwaps = """
    select tx.dc_tx_id as tx_id,
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
    mempool.txn
    from pool_swap 
    inner join token tf on tf.dc_token_id=token_from 
    inner join token tt on tt.dc_token_id=token_to 
    inner join address af on af.row_id = "from"
    inner join address at on at.row_id = "to"
    inner join tx on tx.row_id = pool_swap.tx_row_id
    left join minted_tx on minted_tx.tx_row_id = pool_swap.tx_row_id 
    left join mempool on mempool.tx_row_id = pool_swap.tx_row_id
    left join block on block.height = minted_tx.block_height OR block.height = mempool.block_height + 1
    where 1=1
    order by coalesce(minted_tx.block_height, mempool.block_height) DESC, coalesce(minted_tx.txn, -1)
    limit 100;
""".trimIndent()

val connectionPool = createConnectionPool()

private fun createConnectionPool(): PGSimpleDataSource {
    val connectionPool = PGSimpleDataSource()
    connectionPool.isReadOnly = true
    connectionPool.databaseName = "trader"
    connectionPool.user = "postgres"
    connectionPool.password = "postgres"
    return connectionPool
}

fun createWriteableConnection(): Connection {
    val connection = connectionPool.connection
    connection.autoCommit = false
    connection.isReadOnly = false
    return connection
}

fun useOrReplace(connection: Connection): Connection {
    if (!connection.isValid(1000)) {
        logger.warn("Connection $connection is no longer valid and will be replaced")
        try {
            connection.close()
        } catch (e: Throwable) {
            logger.warn("Suppressed while closing invalid connection", e)
        }
        return createWriteableConnection()
    }
    return connection
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


fun PreparedStatement.setDoubleOrNull(parameterIndex: Int, double: Double?) {
    if (double == null) setNull(parameterIndex, Types.DOUBLE)
    else setDouble(parameterIndex, double)
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
        val conditions = Conditions()
        conditions.addIfPresent("tf.dc_token_symbol = ?", filter.fromTokenSymbol)
        conditions.addIfPresent("tt.dc_token_symbol = ?", filter.toTokenSymbol)

        val filterString = filter.filterString
        if (filterString != null) {

            if (filterString.length < 64) {
                conditions.addIfPresent(
                    "(af.dc_address = ? OR at.dc_address = ?)",
                    filterString
                )
            } else if (filterString.length == 64) {
                conditions.addIfPresent(
                    "(tx.dc_tx_id = ? OR block.hash = ?)",
                    filterString
                )
            }
        }

        if (filter.pager != null) {
            conditions.addIfPresent("", filter.pager.maxBlockHeight)
        }

        val poolSwaps = ArrayList<PoolSwapRow>()
        connectionPool.connection.use {
            it.prepareStatement(conditions.updatedQuery(template_selectPoolSwaps)).use { statement ->
                conditions.setData(1, statement)
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
        }
        return poolSwaps
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
            blockHeight = blockHeight as Long,
            txn = resultSet.getInt(3),
        )

        val blockHeightMempool = resultSet.getObject(12)
        val mempoolEntry = if (blockHeightMempool == null) null else MempoolEntry(
            blockHeight = blockHeightMempool as Long,
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
            block = blockEntry,
            mempool = mempoolEntry
        )
    }

    private class Conditions {

        private val conditions = ArrayList<Condition>()
        private var offset = 0


        private fun addCondition(condition: Condition) {
            conditions.add(condition)
            offset += condition.sql.count { it == '?' }
        }

        fun addIfPresent(sql: String, data: Any?) {
            if (data != null) {
                addCondition(Condition(sql = sql, offset = offset, data = data as Object))
            }
        }

        fun updatedQuery(sql: String): String {
            if (conditions.isEmpty()) {
                return sql
            }
            val whereClause = conditions.joinToString(" AND ") { it.sql }
            return sql.replace("1=1", whereClause)
        }

        fun setData(startIndex: Int, statement: PreparedStatement) {
            for (condition in conditions) {
                condition.setParameter(statement, startIndex)
            }
        }
    }

    @kotlinx.serialization.Serializable
    data class TokenAggregate(
        val tokenSymbol: String,
        val aggregate: Double,
        val txCount: Int,
        val aggregateUSD: Double,
        val mostRecentBlockHeight: Long,
    )

    data class Condition(
        val sql: String,
        val offset: Int,
        val data: Object,
    ) {
        fun setParameter(statement: PreparedStatement, startIndex: Int) {
            val placeholderCount = sql.count { it == '?' }
            repeat(placeholderCount) { n ->
                statement.setObject(startIndex + offset + n, data)
            }
        }
    }

    @kotlinx.serialization.Serializable
    data class BlockEntry(
        val blockHeight: Long,
        val txn: Int,
    )

    @kotlinx.serialization.Serializable
    data class MempoolEntry(
        val blockHeight: Long,
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
    )

    @kotlinx.serialization.Serializable
    data class Pager(
        val maxBlockHeight: Long,
        val minOrdinal: Int,
    )

    @kotlinx.serialization.Serializable
    data class PoolHistoryFilter(
        val fromTokenSymbol: String? = null,
        val toTokenSymbol: String? = null,
        val filterString: String? = null,
        val pager: Pager? = null,
    ) {
        companion object {
            val alphaNumeric = "^[a-zA-Z\\d]+$".toRegex()
            val tokenSymbolRegex = "^[a-zA-Z\\d\\./]+$".toRegex()
        }

        private fun checkTokenSymbol(tokenSymbol: String?) {
            if (tokenSymbol == null) return
            check(tokenSymbol.length < 20 && tokenSymbolRegex.matches(tokenSymbol))
        }

        init {
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
        val blockHeight: Long,
        val blockTime: Long,
        val txn: Int,
        val type: String,
    )
}