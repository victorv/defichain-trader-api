package com.trader.defichain.db.search

import com.trader.defichain.db.DB
import com.trader.defichain.db.connectionPool
import com.trader.defichain.dex.getOraclePriceForSymbol
import com.trader.defichain.util.floorPlain
import org.intellij.lang.annotations.Language
import java.sql.ResultSet
import java.sql.Types

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
            fromTokenID = DB.selectTokenID(connection, filter.fromTokenSymbol)
        }

        if (filter.toTokenSymbol != null) {
            toTokenID = DB.selectTokenID(connection, filter.toTokenSymbol)
        }

        val filterString = filter.filterString
        if (filterString != null) {

            // TODO research if address length can be >= 64
            if (filterString.length != 64) {
                addressRowID = DB.selectAddressRowID(connection, filterString)
            } else {
                blockHeight = DB.selectBlockHeight(connection, filterString)
                if (blockHeight == null) {
                    txRowID = DB.selectTransactionRowID(connection, filterString)
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
    val maxBlockHeight: Long,
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