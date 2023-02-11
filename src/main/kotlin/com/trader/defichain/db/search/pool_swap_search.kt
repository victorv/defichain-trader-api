package com.trader.defichain.db.search

import com.trader.defichain.db.DB
import com.trader.defichain.db.connectionPool
import com.trader.defichain.dex.*
import com.trader.defichain.util.SQLValue
import com.trader.defichain.util.floorPlain
import com.trader.defichain.util.get
import com.trader.defichain.util.prepareStatement
import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.ResultSet
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@Language("sql")
private val statsCommon = """
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
 at.dc_address = '8defichainBurnAddressXXXXXXXdRQkSm' as is_burn,
 amount_from * fop.price as amount_from_usd,
 amount_to * top.price as amount_to_usd
 from pool_swap
 inner join minted_tx m on m.tx_row_id = pool_swap.tx_row_id
 inner join block on block.height = m.block_height
 inner join tx on pool_swap.tx_row_id = tx.row_id
 inner join latest_oracle_price fop on fop.token = token_from or (fop.token = 1 AND token_from = 124)
 inner join latest_oracle_price top on top.token = token_to or (top.token = 1 AND token_to = 124)
 inner join address at on at.row_id = "to"
 where 
  :filter AND
  (:from_address IS NULL or "from" = :from_address) AND
  (:from_address_whitelist_is_null = true or "from" = ANY(:from_address_whitelist)) AND
  (:to_address IS NULL or "to" = :to_address) AND
  (:to_address_whitelist_is_null = true or "to" = ANY(:to_address_whitelist)) AND
  (:min_input_amount IS NULL or amount_from * fop.price >= :min_input_amount) AND
  (:max_input_amount IS NULL or amount_from * fop.price <= :max_input_amount) AND
  (:min_output_amount IS NULL or amount_to * top.price >= :min_output_amount) AND
  (:max_output_amount IS NULL or amount_to * top.price <= :max_output_amount) AND
  (:token_from) AND
  (:token_to)
 order by m.block_height DESC, m.txn limit 26 offset 0)
""".trimIndent()

@Language("sql")
private val template_selectStatsByAddress = """
$statsCommon
select
count(*) as tx_count,
sum(amount_from) as amount_from, 
sum(amount_from_usd) as amount_from_usd, 
sum(amount_to) as amount_to, 
sum(amount_to_usd) as amount_to_usd, 
a.dc_address as address
from pool_swap
inner join swaps on swaps.tx_row_id = pool_swap.tx_row_id
inner join address a on pool_swap."from" = a.row_id
group by a.dc_address
order by sum(amount_from_usd) DESC limit 250;
""".trimIndent()

@Language("sql")
private val template_selectStats = """
$statsCommon
select
count(*) as tx_count,
sum(amount_from) as amount_from, 
sum(amount_from_usd) as amount_from_usd, 
sum(amount_to) as amount_to, 
sum(amount_to_usd) as amount_to_usd, 
token_from, 
token_to,
is_burn
from pool_swap
inner join swaps on swaps.tx_row_id = pool_swap.tx_row_id
group by token_from, token_to, is_burn;
""".trimIndent()

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
 txn,
 fop.price as price_from,
 top.price as price_to
 from pool_swap
 inner join minted_tx m on m.tx_row_id = pool_swap.tx_row_id
 inner join block on block.height = m.block_height
 inner join tx on pool_swap.tx_row_id = tx.row_id
 inner join latest_oracle_price fop on fop.token = token_from or (fop.token = 1 AND token_from = 124)
 inner join latest_oracle_price top on top.token = token_to or (top.token = 1 AND token_to = 124)
 where 
  :filter AND
  (:from_address IS NULL or "from" = :from_address) AND
  (:from_address_whitelist_is_null = true or "from" = ANY(:from_address_whitelist)) AND
  (:to_address IS NULL or "to" = :to_address) AND
  (:to_address_whitelist_is_null = true or "to" = ANY(:to_address_whitelist)) AND
  (:min_input_amount IS NULL or amount_from * fop.price >= :min_input_amount) AND
  (:max_input_amount IS NULL or amount_from * fop.price <= :max_input_amount) AND
  (:min_output_amount IS NULL or amount_to * top.price >= :min_output_amount) AND
  (:max_output_amount IS NULL or amount_to * top.price <= :max_output_amount) AND
  (:token_from) AND
  (:token_to)
 order by m.block_height DESC, m.txn limit 26 offset 0)
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
inner join minted_tx on minted_tx.tx_row_id = pool_swap.tx_row_id
inner join block on block.height = minted_tx.block_height 
left join mempool on mempool.tx_row_id = pool_swap.tx_row_id;
""".trimIndent()

fun getPoolSwapsAsCSV(filter: SearchFilter): String {
    val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")

    val swaps = searchPoolSwaps(filter, DataType.CSV, 5000).results
    val csv = java.lang.StringBuilder()
    csv.append("block")
    csv.append(',')

    csv.append("txn")
    csv.append(',')

    csv.append("median_block_time_utc")
    csv.append(',')

    csv.append("tx_id")
    csv.append(',')

    csv.append("amount_from")
    csv.append(',')

    csv.append("token_from")
    csv.append(',')

    csv.append("amount_to")
    csv.append(',')

    csv.append("token_to")
    csv.append(',')

    csv.append("fee")
    csv.append(',')

    csv.append("max_price")
    csv.append('\n')

    for (swap in swaps) {
        csv.append(swap.block?.blockHeight ?: -1)
        csv.append(',')

        csv.append(swap.block?.txn ?: -1)
        csv.append(',')

        val medianTime = swap.block?.medianTime ?: 0
        val dateTime = LocalDateTime.ofEpochSecond(medianTime, 0, ZoneOffset.UTC)
        csv.append(dateTimeFormat.format(dateTime))
        csv.append(',')

        csv.append(swap.txID)
        csv.append(',')

        csv.append(swap.amountFrom)
        csv.append(',')

        csv.append(swap.tokenFrom)
        csv.append(',')

        csv.append(swap.amountTo)
        csv.append(',')

        csv.append(swap.tokenTo)
        csv.append(',')

        csv.append(swap.fee)
        csv.append(',')

        csv.append(swap.maxPrice)
        csv.append('\n')
    }
    return csv.toString()
}

fun toShortMillis(epoch: Long?): Long? {
    if (epoch != null) {
        return epoch / 1000
    }
    return null
}

fun getStatsByAddress(filter: SearchFilter): List<SwapStatsByAddress> {
    connectionPool.connection.use { connection ->
        val (parameters, sql) = createQuery(filter, connection, template_selectStatsByAddress, 25000)

        connection.prepareStatement(sql, parameters).use { statement ->
            statement.executeQuery().use { resultSet ->

                val stats = ArrayList<SwapStatsByAddress>()
                while (resultSet.next()) {
                    val amountFrom = resultSet.get<Double>("amount_from")
                    val amountSoldUSD = resultSet.get<Double>("amount_from_usd")

                    val amountTo = resultSet.get<Double>("amount_to")
                    val amountBoughtUSD = resultSet.get<Double>("amount_to_usd")

                    val address = resultSet.get<String>("address")
                    val txCount = resultSet.get<Int>("tx_count")

                    stats.add(
                        SwapStatsByAddress(
                            txCount = txCount,
                            address = address,
                            inputAmount = amountFrom,
                            inputAmountUSD = amountSoldUSD,
                            outputAmount = amountTo,
                            outputAmountUSD = amountBoughtUSD
                        )
                    )
                }
                return stats
            }
        }
    }
}


fun getStats(filter: SearchFilter): List<TokenStats> {
    connectionPool.connection.use { connection ->
        val (parameters, sql) = createQuery(filter, connection, template_selectStats, 25000)

        connection.prepareStatement(sql, parameters).use { statement ->
            statement.executeQuery().use { resultSet ->

                val stats = mutableMapOf<String, TokenStats>()
                while (resultSet.next()) {
                    val amountFrom = resultSet.get<Double>("amount_from")
                    val amountSoldUSD = resultSet.get<Double>("amount_from_usd")

                    val amountTo = resultSet.get<Double>("amount_to")
                    val amountBoughtUSD = resultSet.get<Double>("amount_to_usd")

                    val tokenFrom = resultSet.get<Int>("token_from")
                    val tokenTo = resultSet.get<Int>("token_to")
                    val txCount = resultSet.get<Int>("tx_count")
                    val isBurn = resultSet.get<Boolean>("is_burn")
                    val fromSymbol = getTokenSymbol(tokenFrom)
                    var toSymbol = getTokenSymbol(tokenTo)
                    if (isBurn) {
                        toSymbol = "burned $toSymbol"
                    }

                    // TODO swaps like DFI to DFI will count twice?
                    val fromStats = stats.getOrDefault(fromSymbol, TokenStats(fromSymbol))
                    fromStats.soldTXCount += txCount
                    fromStats.amountSold += amountFrom
                    fromStats.amountSoldUSD += amountSoldUSD
                    fromStats.sold.add(
                        SwapStats(
                            txCount = txCount,
                            token = toSymbol,
                            inputAmount = amountFrom,
                            inputAmountUSD = amountSoldUSD,
                            outputAmount = amountTo,
                            outputAmountUSD = amountBoughtUSD
                        )
                    )
                    stats[fromSymbol] = fromStats

                    val toStats = stats.getOrDefault(toSymbol, TokenStats(toSymbol))
                    toStats.boughtTXCount += txCount
                    toStats.amountBought += amountTo
                    toStats.amountBoughtUSD += amountBoughtUSD
                    toStats.bought.add(
                        SwapStats(
                            txCount = txCount,
                            token = fromSymbol,
                            inputAmount = amountFrom,
                            inputAmountUSD = amountSoldUSD,
                            outputAmount = amountTo,
                            outputAmountUSD = amountBoughtUSD
                        )
                    )
                    stats[toSymbol] = toStats
                }

                stats.values.forEach {
                    it.amountNetUSD = it.amountBoughtUSD - it.amountSoldUSD
                    it.amountNet = it.amountBought - it.amountSold
                    it.volumeUSD = it.amountSoldUSD + it.amountBoughtUSD
                    it.volume = it.amountSold + it.amountBought
                    it.sold.sort()
                    it.bought.sort()

                    val tokenStats = mutableMapOf<String, TokenStats>()
                    for (sold in it.sold) {
                        val swapStats = tokenStats.getOrDefault(sold.token, TokenStats(sold.token))
                        swapStats.amountBought += sold.outputAmount
                        swapStats.amountBoughtUSD += sold.outputAmountUSD
                        tokenStats[sold.token] = swapStats
                    }
                    for (bought in it.bought) {
                        val swapStats = tokenStats.getOrDefault(bought.token, TokenStats(bought.token))
                        swapStats.amountSold += bought.inputAmount
                        swapStats.amountSoldUSD += bought.inputAmountUSD
                        tokenStats[bought.token] = swapStats
                    }
                    tokenStats.values.forEach { tokenStat ->
                        tokenStat.amountNetUSD = tokenStat.amountBoughtUSD - tokenStat.amountSoldUSD
                        tokenStat.amountNet = tokenStat.amountBought - tokenStat.amountSold
                        tokenStat.volumeUSD = tokenStat.amountSoldUSD + tokenStat.amountBoughtUSD
                        tokenStat.volume = tokenStat.amountSold + tokenStat.amountBought
                    }
                    it.net.addAll(tokenStats.values.sorted())
                }
                return stats.values.sorted()
            }
        }
    }
}

fun searchPoolSwaps(filter: SearchFilter, dataType: DataType, limit: Int): SimpleSearchResult<PoolSwapRow> {
    val results = ArrayList<PoolSwapRow>()
    connectionPool.connection.use { connection ->
        val (parameters, sql) = createQuery(filter, connection, template_selectPoolSwaps, limit)

        connection.prepareStatement(sql, parameters).use { statement ->
            statement.executeQuery().use { resultSet ->

                while (resultSet.next()) {
                    results.add(
                        getPoolSwapRow(resultSet, dataType)
                    )
                }
            }
        }

        return SimpleSearchResult(results)
    }
}

private fun createQuery(
    filter: SearchFilter,
    connection: Connection,
    template: String,
    limit: Int
): Pair<Map<String, SQLValue>, String> {
    val tokensFrom = getTokenIdentifiers(filter.fromTokenSymbol)
    val tokensTo = getTokenIdentifiers(filter.toTokenSymbol)

    var (sql, parameters) = template.withFilter(connection, filter, limit)

    if (tokensFrom.size == 1 && filter.toTokenSymbol == "is_sold_or_bought") {
        val check = "token_from = ${tokensFrom[0]} or token_to = ${tokensFrom[0]}"
        sql = sql.replace(":token_from", check)
        sql = sql.replace(":token_to", "NULL is NULL")
    } else {
        sql = sql.replace(":token_from", DB.toIsAny("token_from", tokensFrom))
        sql = sql.replace(":token_to", DB.toIsAny("token_to", tokensTo))
    }
    return Pair(parameters, sql)
}

private fun getPoolSwapRow(resultSet: ResultSet, dataType: DataType): PoolSwapRow {
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

    val vAmountFrom = amountFrom.toDouble()
    val vAmountTo = amountTo.toDouble()

    val usdtSwap = if (vAmountFrom == 0.0 || dataType == DataType.CSV) null
    else testPoolSwap(
        PoolSwap(
            amountFrom = vAmountFrom,
            tokenFrom = tokenFrom,
            tokenTo = "USDT",
            desiredResult = 1.0,
        )
    )
    val usdtInverseSwap = if (vAmountTo == 0.0 || dataType == DataType.CSV) null
    else testPoolSwap(
        PoolSwap(
            amountFrom = vAmountTo,
            tokenFrom = tokenTo,
            tokenTo = "USDT",
            desiredResult = 1.0,
        )
    )

    val swap = if (vAmountTo == 0.0 || vAmountFrom == 0.0 || dataType == DataType.CSV) null
    else testPoolSwap(
        PoolSwap(
            amountFrom = vAmountFrom,
            tokenFrom = tokenFrom,
            tokenTo = tokenTo,
            desiredResult = vAmountTo,
        )
    )
    val inverseSwap = if (vAmountTo == 0.0 || vAmountFrom == 0.0 || dataType == DataType.CSV) null
    else testPoolSwap(
        PoolSwap(
            amountFrom = vAmountTo,
            tokenFrom = tokenTo,
            tokenTo = tokenFrom,
            desiredResult = vAmountFrom,
        )
    )

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
        usdtSwap = usdtSwap,
        usdtInverseSwap = usdtInverseSwap,
        swap = swap,
        inverseSwap = inverseSwap,
        priceImpact = 0.0,
        fromAmountUSD = usdtSwap?.estimate ?: 0.0,
        toAmountUSD = usdtInverseSwap?.estimate ?: 0.0,
        blockHeight = blockEntry?.blockHeight ?: mempoolEntry?.blockHeight ?: -1
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
    val usdtSwap: SwapResult? = null,
    val usdtInverseSwap: SwapResult? = null,
    val swap: SwapResult? = null,
    val inverseSwap: SwapResult? = null,
    var priceImpact: Double,
    val fromAmountUSD: Double,
    val toAmountUSD: Double,
    val blockHeight: Int,
)

@kotlinx.serialization.Serializable
data class Pager(
    val maxBlockHeight: Int,
    val blacklist: List<Long>,
)

@kotlinx.serialization.Serializable
data class SwapStatsByAddress(
    val address: String,
    var txCount: Int,
    val inputAmount: Double,
    val inputAmountUSD: Double,
    val outputAmount: Double,
    val outputAmountUSD: Double,
) : Comparable<SwapStats> {
    override fun compareTo(other: SwapStats): Int {
        return if (inputAmountUSD < other.inputAmountUSD) 1 else -1
    }
}

@kotlinx.serialization.Serializable
data class SwapStats(
    val token: String,
    var txCount: Int,
    val inputAmount: Double,
    val inputAmountUSD: Double,
    val outputAmount: Double,
    val outputAmountUSD: Double,
) : Comparable<SwapStats> {
    override fun compareTo(other: SwapStats): Int {
        return if (inputAmountUSD < other.inputAmountUSD) 1 else -1
    }
}

@kotlinx.serialization.Serializable
data class TokenStats(
    val token: String,
    var amountNetUSD: Double = 0.0,
    var amountNet: Double = 0.0,
    var volumeUSD: Double = 0.0,
    var volume: Double = 0.0,
    var soldTXCount: Int = 0,
    var boughtTXCount: Int = 0,
    var amountSold: Double = 0.0,
    var amountSoldUSD: Double = 0.0,
    val sold: MutableList<SwapStats> = mutableListOf(),
    var amountBought: Double = 0.0,
    var amountBoughtUSD: Double = 0.0,
    val bought: MutableList<SwapStats> = mutableListOf(),
    val net: MutableList<TokenStats> = mutableListOf(),
) : Comparable<TokenStats> {
    override fun compareTo(other: TokenStats): Int {
        return if (volumeUSD < other.volumeUSD) 1 else -1
    }
}