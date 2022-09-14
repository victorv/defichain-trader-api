package com.trader.defichain.db

import com.trader.defichain.dex.getOraclePriceForSymbol
import com.trader.defichain.indexer.ZMQTransaction
import com.trader.defichain.util.floorPlain
import org.postgresql.ds.PGSimpleDataSource
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import kotlin.math.absoluteValue

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

private val template_oldestPoolSwapBlock =
    """
    select coalesce(min(block_height), 2147483647) from minted_tx 
    inner join pool_swap on pool_swap.tx_row_id = minted_tx.tx_row_id;
    """.trimIndent()

private val template_latestPoolSwapBlock =
    """
    select coalesce(max(block_height), 0) from minted_tx 
    inner join pool_swap on pool_swap.tx_row_id = minted_tx.tx_row_id;
    """.trimIndent()

private val template_insertTXType =
    """
    INSERT INTO tx_type (dc_tx_type) VALUES (?)
    ON CONFLICT (dc_tx_type) DO UPDATE SET dc_tx_type = tx_type.dc_tx_type
    RETURNING row_id;
    """.trimIndent()

private val template_insertTX =
    """
    INSERT INTO tx (dc_tx_id, tx_type_row_id) VALUES (?, ?)
    ON CONFLICT (dc_tx_id) DO UPDATE SET dc_tx_id = tx.dc_tx_id
    RETURNING row_id;
    """.trimIndent()

private val template_insertUnconfirmedTX =
    """
    INSERT INTO unconfirmed_tx (tx_row_id, hex) VALUES (?, ?)
    ON CONFLICT (tx_row_id) DO NOTHING
    """.trimIndent()

private val template_insertMempoolTX =
    """
    INSERT INTO mempool (tx_row_id, time_received, block_height_received, fee) VALUES (?, ?, ?, ?)
    ON CONFLICT (tx_row_id) DO NOTHING
    """.trimIndent()

private val template_insertMintedTX =
    """
    INSERT INTO minted_tx (tx_row_id, block_time, block_height, ordinal, fee) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (tx_row_id) DO UPDATE SET tx_row_id = minted_tx.tx_row_id
    """.trimIndent()

private val template_insertAddress = """
    INSERT INTO address (dc_address) VALUES (?)
    ON CONFLICT (dc_address) DO UPDATE SET dc_address = address.dc_address
    RETURNING row_id;
    """.trimIndent()

private val template_insertToken = """
    INSERT INTO token (dc_token_id, dc_token_symbol) VALUES (?, ?)
    ON CONFLICT(dc_token_id) DO UPDATE set dc_token_symbol = ?
    """.trimIndent()

private val template_insertPoolSwap = """
    INSERT INTO pool_swap (tx_row_id, "from", "to", token_from, token_to, amount_from, amount_to, max_price) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO UPDATE set amount_to = coalesce(?, pool_swap.amount_to)
    """.trimIndent()

private val template_selectPoolSwaps = """
    select tx.dc_tx_id as tx_id,
    minted_tx.block_height, 
    minted_tx.block_time, 
    minted_tx.ordinal, 
    minted_tx.fee, 
    amount_from, 
    amount_to, 
    tf.dc_token_symbol as token_from, 
    tt.dc_token_symbol as token_to,
    max_price,
    af.dc_address as "from",
    at.dc_address as "to",
    block_height_received,
    time_received,
    mempool.fee as fee_received
    from pool_swap 
    inner join minted_tx on minted_tx.tx_row_id = pool_swap.tx_row_id 
    inner join token tf on tf.dc_token_id=token_from 
    inner join token tt on tt.dc_token_id=token_to 
    inner join address af on af.row_id = "from"
    inner join address at on at.row_id = "to"
    inner join tx on tx.row_id = pool_swap.tx_row_id
    left join mempool on mempool.tx_row_id = pool_swap.tx_row_id
    where 1=1
    order by block_height DESC,ordinal
    limit 100;
""".trimIndent()

private val connectionPool = createConnectionPool()
private val tokens = mutableMapOf<Int, String>()

private fun createConnectionPool(): PGSimpleDataSource {
    val connectionPool = PGSimpleDataSource()
    connectionPool.isReadOnly = true
    connectionPool.databaseName = "trader"
    connectionPool.user = "postgres"
    connectionPool.password = "postgres"
    return connectionPool
}

private fun createWriteableConnection(): Connection {
    val connection = connectionPool.connection
    connection.autoCommit = false
    connection.isReadOnly = false
    return connection
}

private fun useOrReplace(connection: Connection): Connection {
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

private fun insertOrDoNothing(statement: PreparedStatement) {
    val updateCount = statement.executeUpdate()
    check(updateCount == 0 || updateCount == 1)
}

private fun upsertReturning(statement: PreparedStatement): Long {
    statement.executeQuery().use { resultSet ->
        check(resultSet.next())

        val rowID = resultSet.getLong(1)
        check(rowID >= 0)

        check(!resultSet.next())
        return rowID
    }
}

private fun insertTransactionType(updater: DB.Updater, txType: String): Int {
    updater.prepareStatement(template_insertTXType).use {
        it.setString(1, txType)
        return upsertReturning(it).toInt()
    }
}

private fun insertTX(updater: DB.Updater, txID: String, txType: String): Long {
    val txTypeRowID = insertTransactionType(updater, txType)

    updater.prepareStatement(template_insertTX).use {
        it.setString(1, txID)
        it.setInt(2, txTypeRowID)
        return upsertReturning(it)
    }
}

private fun insertMempoolEntry(updater: DB.Updater, txRowID: Long, tx: ZMQTransaction) {
    updater.prepareStatement(template_insertMempoolTX).use {
        it.setLong(1, txRowID)
        it.setLong(2, tx.timeReceived)
        it.setInt(3, tx.blockHeightReceived)
        it.setBigDecimal(4, tx.fee)
        insertOrDoNothing(it)
    }
}

private fun insertToken(updater: DB.Updater, tokenID: Int, tokenSymbol: String) {
    if (tokens.containsKey(tokenID)) {
        return
    }

    updater.prepareStatement(template_insertToken).use {
        it.setInt(1, tokenID)
        it.setString(2, tokenSymbol)
        it.setString(3, tokenSymbol)
        check(it.executeUpdate() <= 1)

        tokens[tokenID] = tokenSymbol
    }
}

private fun insertMintedTX(updater: DB.Updater, txRowID: Long, mintedTX: DB.MintedTX) {
    updater.prepareStatement(template_insertMintedTX).use {
        it.setLong(1, txRowID)
        it.setLong(2, mintedTX.blockTime)
        it.setInt(3, mintedTX.blockHeight)
        it.setInt(4, mintedTX.txOrdinal)
        it.setBigDecimal(5, mintedTX.txFee)
        insertOrDoNothing(it)
    }
}

private fun insertPoolSwap(updater: DB.Updater, txRowID: Long, swap: DB.PoolSwap) {
    val addresses = HashSet<String>()

    insertToken(updater, swap.tokenFrom, tokens.getValue(swap.tokenFrom))
    insertToken(updater, swap.tokenTo, tokens.getValue(swap.tokenTo))

    addresses.add(swap.from)
    addresses.add(swap.to)

    val fromRowID = insertAddress(updater, swap.from)
    val toRowID = insertAddress(updater, swap.to)

    updater.prepareStatement(template_insertPoolSwap).use {
        it.setLong(1, txRowID)
        it.setLong(2, fromRowID)
        it.setLong(3, toRowID)
        it.setInt(4, swap.tokenFrom)
        it.setInt(5, swap.tokenTo)
        it.setDouble(6, swap.amountFrom.absoluteValue)
        it.setDoubleOrNull(7, swap.amountTo?.absoluteValue)
        it.setDouble(8, swap.maxPrice)
        it.setDoubleOrNull(9, swap.amountTo?.absoluteValue)
        check(it.executeUpdate() == 1)
    }
}

private fun insertAddress(
    updater: DB.Updater,
    address: String,
): Long {
    updater.prepareStatement(template_insertAddress).use {
        it.setString(1, address)
        return upsertReturning(it)
    }
}

fun PreparedStatement.setDoubleOrNull(parameterIndex: Int, double: Double?) {
    if (double == null) setNull(parameterIndex, Types.DOUBLE)
    else setDouble(parameterIndex, double)
}

private fun insertUnconfirmedTX(updater: DB.Updater, txRowID: Long, rawTX: String) {
    updater.prepareStatement(template_insertUnconfirmedTX).use {
        it.setLong(1, txRowID)
        it.setString(2, rawTX)
        insertOrDoNothing(it)
    }
}

private fun insertMintedPoolSwap(updater: DB.Updater, mintedTX: DB.MintedTX, swap: DB.PoolSwap) {
    val txRowID = insertTX(updater, mintedTX.txID, "PoolSwap")
    insertMintedTX(updater, txRowID, mintedTX)
    insertPoolSwap(updater, txRowID, swap)
}

object DB {
    fun insertZMQTransactions(updater: Updater, transactions: List<ZMQTransaction>) {
        if (transactions.isEmpty()) return
        updater.doTransaction {
            for (tx in transactions) {
                val txRowID = insertTX(updater, tx.txID, tx.type)
                insertMempoolEntry(updater, txRowID, tx)
                if (tx.poolSwap != null) {
                    insertPoolSwap(updater, txRowID, tx.poolSwap)
                }
                if (!tx.isConfirmed) {
                    insertUnconfirmedTX(updater, txRowID, tx.hex)
                }
            }
        }
    }

    fun insertTokens(updater: Updater, tokens: Map<Int, String>) {
        if (tokens.isEmpty()) return
        updater.doTransaction {
            for ((tokenID, tokenSymbol) in tokens) {
                insertToken(updater, tokenID, tokenSymbol)
            }
        }
    }

    fun insertMintedPoolSwaps(updater: Updater, entries: List<Pair<MintedTX, PoolSwap>>) {
        if (entries.isEmpty()) return
        updater.doTransaction {
            for ((mintedTX, swap) in entries) {
                insertMintedPoolSwap(updater, mintedTX, swap)
            }
        }
    }

    fun getOldestPoolSwapBlock(updater: Updater): Int {
        updater.prepareStatement(template_oldestPoolSwapBlock).use {
            it.executeQuery().use { resultSet ->
                check(resultSet.next())
                return resultSet.getInt(1)
            }
        }
    }

    fun getLatestPoolSwapBlock(updater: Updater): Int {
        updater.prepareStatement(template_latestPoolSwapBlock).use {
            it.executeQuery().use { resultSet ->
                check(resultSet.next())
                return resultSet.getInt(1)
            }
        }
    }

    fun getPoolSwaps(filter: PoolHistoryFilter): List<PoolSwapRow> {
        val conditions = Conditions()
        conditions.addIfPresent("tf.dc_token_symbol = ?", filter.fromTokenSymbol)
        conditions.addIfPresent("tt.dc_token_symbol = ?", filter.toTokenSymbol)
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
            .sortedWith(
                compareByDescending<PoolSwapRow> { it.blockHeight ?: ((it.blockHeightReceived ?: 0) + 1) }
                    .thenBy { it.ordinal ?: 0 }
                    .thenByDescending { (it.fee ?: it.feeReceived)?.toDouble() }
            )
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

    private fun getPoolSwapRow(resultSet: ResultSet) = PoolSwapRow(
        txID = resultSet.getString(1),
        blockHeight = resultSet.getLong(2),
        blockTime = resultSet.getLong(3),
        ordinal = resultSet.getInt(4),
        fee = resultSet.getBigDecimal(5)?.floorPlain(),
        amountFrom = resultSet.getBigDecimal(6).floorPlain(),
        amountTo = resultSet.getBigDecimal(7).floorPlain(),
        tokenFrom = resultSet.getString(8),
        tokenTo = resultSet.getString(9),
        maxPrice = resultSet.getBigDecimal(10).floorPlain(),
        from = resultSet.getString(11),
        to = resultSet.getString(12),
        blockHeightReceived = resultSet.getInt(13),
        timeReceived = resultSet.getLong(14),
        feeReceived = resultSet.getBigDecimal(15)?.floorPlain(),
    )

    fun createUpdater() = Updater(createWriteableConnection())
    data class Updater(
        var connection: Connection,
    ) {
        fun prepareStatement(sql: String): PreparedStatement {
            connection = useOrReplace(connection)
            return connection.prepareStatement(sql)
        }

        inline fun <reified T> doTransaction(run: () -> T): T {
            try {
                val result = run()
                connection.commit()
                return result
            } catch (t: Throwable) {
                try {
                    connection.rollback()
                } catch (rollbackException: Throwable) {
                    t.addSuppressed(rollbackException)
                }
                throw t
            }
        }
    }

    private class Conditions {

        private val conditions = ArrayList<Condition>()
        private var offset = 0


        private fun addCondition(condition: Condition) {
            conditions.add(condition)
            offset++
        }

        fun addIfPresent(sql: String, data: String?) {
            if (data != null) {
                addCondition(StringCondition(sql = sql, offset = offset, data = data))
            }
        }

        fun addIfPresent(sql: String, data: Int?) {
            if (data != null) {
                addCondition(IntCondition(sql = sql, offset = offset, data = data))
            }
        }

        fun addIfPresent(sql: String, data: Long?) {
            if (data != null) {
                addCondition(LongCondition(sql = sql, offset = offset, data = data))
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

    private interface Condition {
        val sql: String
        val offset: Int

        fun setParameter(statement: PreparedStatement, startIndex: Int)
    }

    data class StringCondition(
        override val sql: String,
        override val offset: Int,
        val data: String,
    ) : Condition {

        override fun setParameter(statement: PreparedStatement, startIndex: Int) {
            statement.setString(startIndex + offset, data)
        }
    }

    data class IntCondition(
        override val sql: String,
        override val offset: Int,
        val data: Int,
    ) : Condition {

        override fun setParameter(statement: PreparedStatement, startIndex: Int) {
            statement.setInt(startIndex + offset, data)
        }
    }

    data class LongCondition(
        override val sql: String,
        override val offset: Int,
        val data: Long,
    ) : Condition {

        override fun setParameter(statement: PreparedStatement, startIndex: Int) {
            statement.setLong(startIndex + offset, data)
        }
    }


    @kotlinx.serialization.Serializable
    data class PoolSwapRow(
        val txID: String,
        val blockHeight: Long?,
        val blockTime: Long?,
        val ordinal: Int?,
        val fee: String?,
        val amountFrom: String,
        val amountTo: String,
        val tokenFrom: String,
        val tokenTo: String,
        val maxPrice: String,
        val from: String,
        val to: String,
        val blockHeightReceived: Int?,
        val timeReceived: Long?,
        val feeReceived: String?,
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
        val pager: Pager? = null,
    ) {
        companion object {
            val tokenSymbolRegex = "^[a-zA-Z\\d\\./]+$".toRegex()
        }

        private fun checkTokenSymbol(tokenSymbol: String?) {
            if (tokenSymbol == null) return
            check(tokenSymbol.length < 20 && tokenSymbolRegex.matches(tokenSymbol))
        }

        init {
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
        val txFee: BigDecimal,
        val txOrdinal: Int,
    )
}