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
amount,
token.dc_token_symbol as token,
address.dc_address as "owner",
minted_tx.block_height,
minted_tx.txn,
vault.dc_vault_id as vault,
fee,
time
from loan 
inner join minted_tx ON minted_tx.tx_row_id = loan.tx_row_id
inner join address ON address.row_id = loan."owner"
inner join tx ON tx.row_id = loan.tx_row_id
inner join vault ON vault.row_id = loan.vault
inner join tx_type on tx.tx_type_row_id = tx_type.row_id
inner join block ON block.height = minted_tx.block_height
inner join token_amount on token_amount.tx_row_id = loan.tx_row_id
inner join token token ON token.dc_token_id = token_amount.token
where 
  :filter AND
  (:from_address IS NULL or loan.owner = :from_address) AND
  (:from_address_whitelist_is_null = true or loan.owner = ANY(:from_address_whitelist)) AND
  (:pager_block_height IS NULL or block_height <= :pager_block_height) AND
  (loan.tx_row_id not in :blacklisted)
order by minted_tx.block_height DESC, minted_tx.txn limit 26 offset 0
""".trimIndent()

fun getLoansAsCSV(filter: SearchFilter): String {
    val dateTimeFormat = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN)

    val results = searchLoans(filter, 5000).results
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

    csv.append("amount")
    csv.append(',')

    csv.append("token")
    csv.append(',')

    csv.append("vault")
    csv.append(',')

    csv.append("owner")
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

        csv.append(result.amount)
        csv.append(',')

        csv.append(result.token)
        csv.append(',')

        csv.append(result.vault)
        csv.append(',')

        csv.append(result.owner)
        csv.append(',')

        csv.append(result.fee)
        csv.append('\n')
    }
    return csv.toString()
}

fun searchLoans(filter: SearchFilter, limit: Int): SimpleSearchResult<Loan> {
    val results = ArrayList<Loan>()
    connectionPool.connection.use { connection ->
        val (sql, parameters) = query.withFilter(connection, filter, limit)

        connection.prepareStatement(sql, parameters).use { statement ->
            statement.executeQuery().use { resultSet ->

                while (resultSet.next()) {
                    results.add(
                        Loan(
                            id = resultSet.getLong("id"),
                            txID = resultSet.getString("tx_id"),
                            type = resultSet.getString("type"),
                            amount = resultSet.getDouble("amount"),
                            token = resultSet.getString("token"),
                            vault = resultSet.getString("vault"),
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
data class Loan(
    val id: Long,
    val txID: String,
    val amount: Double,
    val token: String,
    val vault: String,
    val owner: String,
    val blockHeight: Int,
    val time: Long,
    val txn: Int,
    val fee: Double,
    val type: String,
)