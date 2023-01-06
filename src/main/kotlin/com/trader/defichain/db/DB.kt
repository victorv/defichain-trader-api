package com.trader.defichain.db

import kotlinx.serialization.json.*
import org.intellij.lang.annotations.Language
import org.postgresql.ds.PGSimpleDataSource
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement

@Language("sql")
private val template_boughtSoldByAddress = """
with latest_oracle as (
select token, max(block_height) as block_height from oracle_price group by token
),
latest_oracle_price as (
select oracle_price.token, oracle_price.price from oracle_price 
inner join latest_oracle lo on lo.token=oracle_price.token AND lo.block_height=oracle_price.block_height
),
bought_sold as (
select 
	pool_swap."from",
	token_from,
	token_to,
    sum(amount_from) as sold,
	sum(amount_to) as bought,
	count(*) as tx_count, 
	max(time) as block_time 
	from minted_tx
inner join pool_swap on pool_swap.tx_row_id = minted_tx.tx_row_id
inner join block on block.height = block_height
where block_height >= (select max(block_height) from minted_tx) - :period
group by pool_swap."from", token_from, token_to
), 
bought_sold_usd as (
select 
address.dc_address,
sold * so.price as sold_usd,
bought * bo.price as bought_usd,
tx_count
from bought_sold
inner join address on address.row_id = "from"
inner join latest_oracle_price so on so.token = token_from AND :token_from
inner join latest_oracle_price bo on bo.token = token_to AND :token_to
),
bought_sold_agg as (
select 
dc_address, 
sum(sold_usd) as sold_usd,
sum(bought_usd) as bought_usd,
sum(tx_count) as tx_count
from bought_sold_usd
group by dc_address
)
select 
*,
bought_usd - sold_usd as net_usd,
bought_usd + sold_usd as total
from bought_sold_agg
order by (bought_usd + sold_usd) DESC
limit 250;
""".trimIndent()

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
coalesce(b.amount, 0) - coalesce(s.amount, 0) net, 
coalesce(b.amount_usd, 0) - coalesce(s.amount_usd, 0) net_usd,
coalesce(b.dc_token_symbol, s.dc_token_symbol) as token_symbol
from sold_usd s full outer join bought_usd b on b.dc_token_symbol=s.dc_token_symbol;
""".trimIndent()

private val templates = mapOf(
    "bought_sold" to template_boughtSold,
    "bought_sold_by_address" to template_boughtSoldByAddress
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
        for (record in selectAllRecords(query)) {
            results.add(Json.decodeFromJsonElement(record))
        }
        return results
    }

    inline fun selectAllRecords(query: String): MutableList<JsonElement> {
        val results = ArrayList<JsonElement>()
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

    fun selectBlockHeight(
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

    fun selectTransactionRowID(
        connection: Connection,
        txID: String?,
    ): Long? {
        if (txID == null) {
            return null
        }

        val sql = "select row_id from tx where dc_tx_id = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, txID)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return null
                return resultSet.getLong(1)
            }
        }
    }

    fun selectAddresses(
        connection: Connection,
        addresses: List<String>?,
    ): java.sql.Array? {
        if (addresses == null || addresses.isEmpty()) {
            return null
        }

        val addressIdentifiers = ArrayList<Long>()
        val addressArray = connection.createArrayOf("VARCHAR", addresses.toTypedArray())
        val sql = "select row_id from address where dc_address = ANY(?)"
        connection.prepareStatement(sql).use { statement ->
            statement.setArray(1, addressArray)
            statement.executeQuery().use { resultSet ->
                while (resultSet.next()) {
                    addressIdentifiers.add(resultSet.getLong(1))
                }
            }
        }
        if(addressIdentifiers.isEmpty()) {
            return null
        }

        return connection.createArrayOf("BIGINT", addressIdentifiers.toTypedArray())
    }

    fun selectAddressRowID(
        connection: Connection,
        address: String?,
    ): Long? {
        if (address == null) {
            return null
        }

        val sql = "select row_id from address where dc_address = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, address)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return -1
                return resultSet.getLong(1)
            }
        }
    }

    fun selectTokenID(
        connection: Connection,
        tokenSymbol: String?,
    ): Int? {
        if (tokenSymbol == null) {
            return null
        }

        val sql = "select dc_token_id from token where dc_token_symbol = ?"
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, tokenSymbol)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) return null
                return resultSet.getInt(1)
            }
        }
    }

    fun toIsAny(columnName: String, tokenIdentifiers: Array<Int>): String {
        if (tokenIdentifiers.isEmpty()) {
            return "NULL IS NULL"
        }
        return "$columnName IN (${tokenIdentifiers.joinToString(",") { it.toString() }})"
    }

    fun stats(templateName: String, period: Int, tokensFrom: Array<Int>, tokensTo: Array<Int>): List<JsonObject> {
        val template = templates.getValue(templateName)
        var sql = template.replace(":period", period.toString())
        sql = sql.replace(":token_from", toIsAny("token_from", tokensFrom))
        sql = sql.replace(":token_to", toIsAny("token_to", tokensTo))
        return selectAllRecords(sql).map { it.jsonObject }
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