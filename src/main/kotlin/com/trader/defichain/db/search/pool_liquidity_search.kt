package com.trader.defichain.db.search

import com.trader.defichain.db.connectionPool
import com.trader.defichain.util.DATE_TIME_PATTERN
import com.trader.defichain.util.prepareStatement
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@Language("sql")
private val query = """
select 
tx.row_id as id,
tx_type.dc_tx_type as "type",
tx.dc_tx_id as tx_id,
amount_a,
token1.dc_token_symbol as token_a,
amount_b,
token2.dc_token_symbol as token_b,
shares,
address.dc_address as "owner",
minted_tx.block_height,
minted_tx.txn,
fee,
time
from pool_liquidity 
inner join minted_tx ON minted_tx.tx_row_id = pool_liquidity.tx_row_id
inner join token token1 ON token1.dc_token_id = pool_liquidity.token_a
inner join token token2 ON token2.dc_token_id = pool_liquidity.token_b
inner join address ON address.row_id = pool_liquidity."owner"
inner join tx ON tx.row_id = pool_liquidity.tx_row_id
inner join tx_type on tx.tx_type_row_id = tx_type.row_id
inner join block ON block.height = minted_tx.block_height
where 
  :filter AND
  (:from_address IS NULL or owner = :from_address) AND
  (:from_address_whitelist_is_null = true or owner = ANY(:from_address_whitelist)) AND
  (:pager_block_height IS NULL or block_height <= :pager_block_height) AND
  (pool_liquidity.tx_row_id not in :blacklisted)
order by minted_tx.block_height DESC, minted_tx.txn limit 26 offset 0
""".trimIndent()

fun getPoolLiquidityAsCSV(filter: SearchFilter): String {
    val dateTimeFormat = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN)

    val results = searchPoolLiquidity(filter, 5000).results
    val csv = java.lang.StringBuilder()
    csv.append("type")
    csv.append(",")

    csv.append("block")
    csv.append(',')

    csv.append("txn")
    csv.append(',')

    csv.append("median_block_time_utc")
    csv.append(',')

    csv.append("tx_id")
    csv.append(',')

    csv.append("amount_a")
    csv.append(',')

    csv.append("token_a")
    csv.append(',')

    csv.append("amount_b")
    csv.append(',')

    csv.append("token_b")
    csv.append(',')

    csv.append("owner")
    csv.append(',')

    csv.append("shares")
    csv.append(',')

    csv.append("fee")
    csv.append('\n')

    for (result in results) {
        csv.append(result.type)
        csv.append(',')

        csv.append(result.blockHeight)
        csv.append(',')

        csv.append(result.txn)
        csv.append(',')

        val dateTime = LocalDateTime.ofEpochSecond(result.time, 0, ZoneOffset.UTC)
        csv.append(dateTimeFormat.format(dateTime))
        csv.append(',')

        csv.append(result.txID)
        csv.append(',')

        csv.append(result.amountA)
        csv.append(',')

        csv.append(result.tokenA)
        csv.append(',')

        csv.append(result.amountB)
        csv.append(',')

        csv.append(result.tokenB)
        csv.append(',')

        csv.append(result.owner)
        csv.append(',')

        csv.append(result.shares)
        csv.append(',')

        csv.append(result.fee)
        csv.append('\n')
    }
    return csv.toString()
}

fun searchPoolLiquidity(filter: SearchFilter, limit: Int): SimpleSearchResult<PoolLiquidity> {
    val results = ArrayList<PoolLiquidity>()
    connectionPool.connection.use { connection ->
        val (sql, parameters) = query.withFilter(connection, filter, limit)

        connection.prepareStatement(sql, parameters).use { statement ->
            statement.executeQuery().use { resultSet ->

                while (resultSet.next()) {
                    results.add(
                        PoolLiquidity(
                            id = resultSet.getLong("id"),
                            txID = resultSet.getString("tx_id"),
                            type = resultSet.getString("type"),
                            amountA = resultSet.getDouble("amount_a"),
                            amountB = resultSet.getDouble("amount_b"),
                            tokenA = resultSet.getString("token_a"),
                            tokenB = resultSet.getString("token_b"),
                            shares = resultSet.getDouble("shares"),
                            owner = resultSet.getString("owner"),
                            blockHeight = resultSet.getInt("block_height"),
                            time = resultSet.getLong("time"),
                            txn = resultSet.getInt("txn"),
                            fee = resultSet.getDouble("fee"),
                        )
                    )
                }
            }
        }
        return SimpleSearchResult(results)
    }
}

@kotlinx.serialization.Serializable
data class PoolLiquidity(
    val id: Long,
    val txID: String,
    val amountA: Double,
    val tokenA: String,
    val amountB: Double,
    val tokenB: String,
    val shares: Double,
    val owner: String,
    val blockHeight: Int,
    val time: Long,
    val txn: Int,
    val fee: Double,
    val type: String,
)