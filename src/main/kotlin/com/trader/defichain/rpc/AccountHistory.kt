package com.trader.defichain.rpc

import com.trader.defichain.indexer.TokenIndex
import kotlinx.serialization.json.JsonPrimitive

object AccountHistory {

    suspend fun getPoolLiquidityShares(
        owner: String,
        blockHeight: Long,
        txn: Int
    ): TokenIndex.TokenAmount {
        val record = getRecord<PoolLiquidity>(owner, blockHeight, txn)
        val shares = record.amounts.first { it.split("@").last().indexOf("-") > 0 }
        return TokenIndex.decodeTokenAmount(shares)
    }

    suspend fun getPoolLiquidityAmounts(
        owner: String,
        blockHeight: Long,
        txn: Int,
        poolID: Int,
    ): PoolLiquidityAmounts {
        val record = getRecord<PoolLiquidity>(owner, blockHeight, txn)

        val pool = TokenIndex.getPoolPair(poolID)

        val amountA = record.amounts
            .first { it.endsWith(TokenIndex.getSymbol(pool.idTokenA.toInt())) }
        val amountB = record.amounts
            .first { it.endsWith(TokenIndex.getSymbol(pool.idTokenB.toInt())) }

        return PoolLiquidityAmounts(
            amountA = TokenIndex.decodeTokenAmount(amountA),
            amountB = TokenIndex.decodeTokenAmount(amountB),
        )
    }

    suspend fun getPoolSwapResultFor(poolSwap: CustomTX.PoolSwap, blockHeight: Long, txn: Int): Double {
        val amounts = getPoolSwapAmounts(poolSwap.fromAddress, blockHeight, txn).toMutableList()

        val firstAmount = amounts.first()

        check(
            firstAmount.amount < 0.0
                    && -firstAmount.amount == poolSwap.fromAmount
                    && firstAmount.tokenID == poolSwap.fromToken
        )

        if (poolSwap.fromAddress != poolSwap.toAddress) {
            check(amounts.size == 1)
            amounts += getPoolSwapAmounts(poolSwap.toAddress, blockHeight, txn)
            check(amounts.size <= 2)
        }

        if (amounts.size == 1) {
            return 0.0
        }

        val lastAmount = amounts.last()
        check(amounts.size == 2 && lastAmount.amount >= 0.0 && lastAmount.tokenID == poolSwap.toToken)
        return lastAmount.amount
    }

    private suspend fun getPoolSwapAmounts(owner: String, blockHeight: Long, txn: Int): List<TokenIndex.TokenAmount> {
        val swap = getRecord<PoolSwap>(owner, blockHeight, txn)

        check(swap.type == "PoolSwap")

        val amounts = swap.amounts.map {
            TokenIndex.decodeTokenAmount(it)
        }.sortedBy { it.amount }

        check(amounts.size in 1..2)
        return amounts
    }

    private suspend inline fun <reified T : Record> getRecord(
        owner: String,
        blockHeight: Long,
        txn: Int
    ): T {
        val record = RPC.getValue<T>(
            RPCMethod.GET_ACCOUNT_HISTORY,
            JsonPrimitive(owner),
            JsonPrimitive(blockHeight),
            JsonPrimitive(txn)
        )

        check(record.owner == owner)
        check(record.blockHeight == blockHeight)
        check(record.txn == txn)
        return record
    }

    data class PoolLiquidityAmounts(
        val amountA: TokenIndex.TokenAmount,
        val amountB: TokenIndex.TokenAmount,
    )

    interface Record {
        val type: String
        val owner: String
        val blockHeight: Long
        val txn: Int
    }

    @kotlinx.serialization.Serializable
    data class PoolSwap(
        val amounts: List<String>,
        override val type: String,
        override val owner: String,
        override val blockHeight: Long,
        override val txn: Int,
    ) : Record

    @kotlinx.serialization.Serializable
    data class PoolLiquidity(
        val amounts: List<String>,
        override val type: String,
        override val owner: String,
        override val blockHeight: Long,
        override val txn: Int,
    ) : Record
}