package com.trader.defichain.db.search

import com.trader.defichain.db.DB
import com.trader.defichain.db.connectionPool
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.getOraclePriceForSymbol
import com.trader.defichain.dex.getTokenIdentifiers
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.util.SQLValue
import com.trader.defichain.util.floorPlain
import com.trader.defichain.util.prepareStatement
import io.ktor.server.application.*
import org.intellij.lang.annotations.Language
import java.sql.ResultSet
import java.sql.Types

@Language("sql")
private val template_selectPoolSwaps = """
with latest_oracle as (
select token, max(block_height) as block_height from oracle_price group by token
),
latest_oracle_price as (
select oracle_price.token, oracle_price.price from oracle_price 
inner join latest_oracle lo on lo.token=oracle_price.token AND lo.block_height=oracle_price.block_height
),    
swaps as (
 select 
 pool_swap.tx_row_id,
 block_height,
 txn
 from pool_swap
 inner join minted_tx m on m.tx_row_id = pool_swap.tx_row_id
 inner join tx on pool_swap.tx_row_id = tx.row_id
 inner join latest_oracle_price fop on fop.token = token_from
 inner join latest_oracle_price top on top.token = token_to
 where 
  (:pager_block_height IS NULL or block_height <= :pager_block_height) AND
  pool_swap.tx_row_id <> ANY(:blacklisted) AND
  (:min_block_height IS NULL or block_height >= :min_block_height) AND
  (:max_block_height IS NULL or block_height <= :max_block_height) AND
  (:min_fee IS NULL or fee >= :min_fee) AND
  (:max_fee IS NULL or fee <= :max_fee) AND
  (:min_input_amount IS NULL or amount_from * fop.price >= :min_input_amount) AND
  (:max_input_amount IS NULL or amount_from * fop.price <= :max_input_amount) AND
  (:min_output_amount IS NULL or amount_to * top.price >= :min_output_amount) AND
  (:max_output_amount IS NULL or amount_to * top.price <= :max_output_amount) AND
  (:token_from) AND
  (:token_to) AND
  (:from_address IS NULL or "from" = :from_address) AND
  (:from_address_whitelist_is_null = true or "from" = ANY(:from_address_whitelist)) AND
  (:to_address IS NULL or "to" = :to_address) AND
  (:to_address_whitelist_is_null = true or "to" = ANY(:to_address_whitelist)) AND
  (:tx_id IS NULL or pool_swap.tx_row_id = :tx_id)
 order by m.block_height DESC, m.txn
 limit 26 offset 0
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
tx.row_id,
block.time
from pool_swap
inner join swaps on swaps.tx_row_id = pool_swap.tx_row_id
inner join token tf on tf.dc_token_id=token_from 
inner join token tt on tt.dc_token_id=token_to
inner join token tta on tta.dc_token_id=token_to_alt
inner join address af on af.row_id = "from"
inner join address at on at.row_id = "to"
inner join tx on tx.row_id = pool_swap.tx_row_id
left join minted_tx on minted_tx.tx_row_id = pool_swap.tx_row_id
left join block on block.height = minted_tx.block_height 
left join mempool on mempool.tx_row_id = pool_swap.tx_row_id;
""".trimIndent()

fun getPoolSwaps(filter: PoolHistoryFilter, limit: Boolean = true): List<PoolSwapRow> {
    connectionPool.connection.use { connection ->
        var pagerBlockHeight: Int? = null
        var blacklist = arrayOf<Long>(-1)
        if (filter.pager != null) {
            pagerBlockHeight = filter.pager.maxBlockHeight
            blacklist = filter.pager.blacklist.toTypedArray()
        }
        val blacklistArray = connection.createArrayOf("BIGINT", blacklist)

        val tokensFrom = getTokenIdentifiers(filter.fromTokenSymbol)
        val tokensTo = getTokenIdentifiers(filter.toTokenSymbol)
        val fromAddress = DB.selectAddressRowID(connection, filter.fromAddress)
        val toAddress = DB.selectAddressRowID(connection, filter.toAddress)
        val fromAddressWhitelist = DB.selectAddresses(connection, filter.fromAddressGroup)
        val toAddressWhitelist = DB.selectAddresses(connection, filter.toAddressGroup)
        val txRowID = DB.selectTransactionRowID(connection, filter.txID)

        val parameters = mapOf(
            "from_address" to SQLValue(fromAddress, Types.BIGINT),
            "to_address" to SQLValue(toAddress, Types.BIGINT),
            "from_address_whitelist" to SQLValue(fromAddressWhitelist, Types.ARRAY, "BIGINT"),
            "from_address_whitelist_is_null" to SQLValue(fromAddressWhitelist == null, Types.BOOLEAN),
            "to_address_whitelist" to SQLValue(toAddressWhitelist, Types.ARRAY, "BIGINT"),
            "to_address_whitelist_is_null" to SQLValue(toAddressWhitelist == null, Types.BOOLEAN),
            "min_block_height" to SQLValue(filter.minBlock, Types.INTEGER),
            "max_block_height" to SQLValue(filter.maxBlock, Types.INTEGER),
            "min_fee" to SQLValue(filter.minFee, Types.NUMERIC),
            "max_fee" to SQLValue(filter.maxFee, Types.NUMERIC),
            "min_input_amount" to SQLValue(filter.minInputAmount, Types.NUMERIC),
            "max_input_amount" to SQLValue(filter.maxInputAmount, Types.NUMERIC),
            "min_output_amount" to SQLValue(filter.minOutputAmount, Types.NUMERIC),
            "max_output_amount" to SQLValue(filter.maxOutputAmount, Types.NUMERIC),
            "tx_id" to SQLValue(txRowID, Types.BIGINT),
            "pager_block_height" to SQLValue(pagerBlockHeight, Types.INTEGER),
            "blacklisted" to SQLValue(blacklistArray, Types.ARRAY),
        )

        val poolSwaps = ArrayList<PoolSwapRow>()
        var sql = if(!limit) template_selectPoolSwaps.replace("limit 26", "limit 250") else template_selectPoolSwaps
        sql = sql.replace(":token_from", DB.toIsAny("token_from", tokensFrom))
        sql = sql.replace(":token_to", DB.toIsAny("token_to", tokensTo))

        connection.prepareStatement(sql, parameters).use { statement ->
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

private fun getPoolSwapRow(resultSet: ResultSet): PoolSwapRow {
    val blockHeight = resultSet.getObject(2)
    val blockEntry = if (blockHeight == null) null else BlockEntry(
        blockHeight = blockHeight as Int,
        txn = resultSet.getInt(3),
        medianTime = resultSet.getLong(17),
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

    val fromAmountUSD = if(amountFrom.toDouble() == 0.0) 0.0 else testPoolSwap(PoolSwap(
        amountFrom = amountFrom.toDouble(),
        tokenFrom = tokenFrom,
        tokenTo = "USDT",
        desiredResult = 1.0,
    )).estimate
    val toAmountUSD = if(amountTo.toDouble() == 0.0) 0.0 else testPoolSwap(PoolSwap(
        amountFrom = amountTo.toDouble(),
        tokenFrom = tokenTo,
        tokenTo = "USDT",
        desiredResult = 1.0,
    )).estimate

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
        fromAmountUSD = if (tokenFrom == "USDT") amountFrom.toDouble() else fromAmountUSD,
        toAmountUSD = if (tokenTo == "USDT") amountTo.toDouble() else toAmountUSD,
        priceImpact = 0.0,
    )
}

@kotlinx.serialization.Serializable
data class BlockEntry(
    val blockHeight: Int,
    val txn: Int,
    val medianTime: Long,
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
    val maxBlockHeight: Int,
    val blacklist: List<Long>,
)

@kotlinx.serialization.Serializable
data class PoolHistoryFilter(
    val txID: String? = null,
    val minBlock: Int? = null,
    val maxBlock: Int? = null,
    val minInputAmount: Double? = null,
    val maxInputAmount: Double? = null,
    val minOutputAmount: Double? = null,
    val maxOutputAmount: Double? = null,
    val minFee: Double? = null,
    val maxFee: Double? = null,
    val fromAddressGroup: List<String>? = null,
    val fromAddress: String? = null,
    val toAddressGroup: List<String>? = null,
    val toAddress: String? = null,
    val fromTokenSymbol: String? = null,
    val toTokenSymbol: String? = null,
    val pager: Pager? = null,
) {
    companion object {

        val tokenSymbolRegex = "^[a-zA-Z\\d\\./_]+$".toRegex()
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