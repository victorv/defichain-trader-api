package com.trader.defichain.db.search

import com.trader.defichain.db.DB
import com.trader.defichain.util.SQLValue
import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.Types

@Language("sql")
private val filterSQL = """
  (:pager_block_height IS NULL or block_height <= :pager_block_height) AND
  (tx.row_id not in :blacklisted) AND
  (:min_date IS NULL or block.time >= :min_date) AND
  (:max_date IS NULL or block.time <= :max_date) AND
  (:min_block_height IS NULL or block_height >= :min_block_height) AND
  (:max_block_height IS NULL or block_height <= :max_block_height) AND
  (:min_fee IS NULL or fee >= :min_fee) AND
  (:max_fee IS NULL or fee <= :max_fee) AND
  (:tx_id IS NULL or tx.row_id = :tx_id)
""".trimIndent()

fun String.withFilter(
    connection: Connection,
    filter: SearchFilter,
    limit: Int?
): Pair<String, MutableMap<String, SQLValue>> {
    val fromAddress = DB.selectAddressRowID(connection, filter.fromAddress)
    val toAddress = DB.selectAddressRowID(connection, filter.toAddress)
    val fromAddressWhitelist = DB.selectAddresses(connection, filter.fromAddressGroup)
    val toAddressWhitelist = DB.selectAddresses(connection, filter.toAddressGroup)
    val txRowID = DB.selectTransactionRowID(connection, filter.txID)

    val pager = filter.pager
    val parameters = mutableMapOf(
        "min_block_height" to SQLValue(filter.minBlock, Types.INTEGER),
        "max_block_height" to SQLValue(filter.maxBlock, Types.INTEGER),
        "from_address" to SQLValue(fromAddress, Types.BIGINT),
        "to_address" to SQLValue(toAddress, Types.BIGINT),
        "from_address_whitelist" to SQLValue(fromAddressWhitelist, Types.ARRAY, "BIGINT"),
        "from_address_whitelist_is_null" to SQLValue(fromAddressWhitelist == null, Types.BOOLEAN),
        "to_address_whitelist" to SQLValue(toAddressWhitelist, Types.ARRAY, "BIGINT"),
        "to_address_whitelist_is_null" to SQLValue(toAddressWhitelist == null, Types.BOOLEAN),
        "min_date" to SQLValue(toShortMillis(filter.minDate), Types.BIGINT),
        "max_date" to SQLValue(toShortMillis(filter.maxDate), Types.BIGINT),
        "tx_id" to SQLValue(txRowID, Types.BIGINT),
        "min_fee" to SQLValue(filter.minFee, Types.NUMERIC),
        "max_fee" to SQLValue(filter.maxFee, Types.NUMERIC),
        "min_input_amount" to SQLValue(filter.minInputAmount, Types.NUMERIC),
        "max_input_amount" to SQLValue(filter.maxInputAmount, Types.NUMERIC),
        "min_output_amount" to SQLValue(filter.minOutputAmount, Types.NUMERIC),
        "max_output_amount" to SQLValue(filter.maxOutputAmount, Types.NUMERIC),
        "pager_block_height" to SQLValue(pager?.maxBlockHeight, Types.INTEGER),
    )
    val sql = this.replace("limit 26", "limit $limit")
        .replace(":filter", filterSQL)


    val blacklist = pager?.blacklist ?: listOf<Long>(-1)
    val blacklistString = blacklist.joinToString(",")
    return sql.replace(":blacklisted", "($blacklistString)") to parameters
}

@kotlinx.serialization.Serializable
data class SimpleSearchResult<T>(
    val results: List<T>
)

@kotlinx.serialization.Serializable
data class SearchFilter(
    val id: String? = null,
    var txID: String? = null,
    val minDate: Long? = null,
    val maxDate: Long? = null,
    var minBlock: Int? = null,
    var maxBlock: Int? = null,
    val minInputAmount: Double? = null,
    val maxInputAmount: Double? = null,
    val minOutputAmount: Double? = null,
    val maxOutputAmount: Double? = null,
    val minFee: Double? = null,
    val maxFee: Double? = null,
    val fromAddressGroup: List<String>? = null,
    var fromAddress: String? = null,
    val toAddressGroup: List<String>? = null,
    var toAddress: String? = null,
    val fromTokenSymbol: String? = null,
    val toTokenSymbol: String? = null,
    val query: String? = null,
    val pager: Pager? = null,
) {
    companion object {

        val tokenSymbolRegex = "^[a-zA-Z\\d\\./_]+$".toRegex()
        val blockHeightRegex = "^\\d+$".toRegex()
    }

    private fun checkTokenSymbol(tokenSymbol: String?) {
        if (tokenSymbol == null) return
        check(tokenSymbol.length < 20 && tokenSymbolRegex.matches(tokenSymbol))
    }

    init {
        checkTokenSymbol(fromTokenSymbol)
        checkTokenSymbol(toTokenSymbol)

        if (query != null && query.isNotBlank()) {
            if (query.length == 64) {
                txID = query
            } else if (blockHeightRegex.matches(query)) {
                val blockHeight = query.toInt()
                minBlock = blockHeight
                maxBlock = blockHeight
            } else if (query.length < 100) {
                fromAddress = query
            }
        }
    }
}

enum class DataType {
    CSV,
    LIST
}