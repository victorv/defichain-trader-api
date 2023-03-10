package com.trader.defichain.rpc

import com.trader.defichain.indexer.TokenIndex
import com.trader.defichain.util.err
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement

private val decoder = Json { ignoreUnknownKeys = true }

object AccountHistory {

    suspend fun getPoolLiquidityShares(
        addPoolLiquidity: CustomTX.AddPoolLiquidity,
        blockHeight: Int,
        txn: Int
    ): TokenIndex.TokenAmount? {
        val record = getRecord<PoolLiquidity>(addPoolLiquidity.owner, blockHeight, txn)
        if (record != null) {
            val shares = record.amounts.first { it.split("@").last().indexOf("-") > 0 }
            return TokenIndex.decodeTokenAmount(shares)
        }
        return null
    }

    suspend fun getPoolLiquidityAmounts(
        owner: String,
        blockHeight: Int,
        txn: Int,
        idTokenA: Int,
        idTokenB: Int,
    ): PoolLiquidityAmounts {
        val record = getRecord<PoolLiquidity>(owner, blockHeight, txn)

        if (record != null) {
            val amountA = record.amounts
                .firstOrNull { it.endsWith("@${TokenIndex.getSymbol(idTokenA)}") }
            val amountB = record.amounts
                .firstOrNull { it.endsWith("@${TokenIndex.getSymbol(idTokenB)}") }

            return PoolLiquidityAmounts(
                amountA = if (amountA != null) TokenIndex.decodeTokenAmount(amountA)
                else TokenIndex.TokenAmount(idTokenA, null),
                amountB = if (amountB != null) TokenIndex.decodeTokenAmount(amountB)
                else TokenIndex.TokenAmount(idTokenB, null),
            )
        }

        return PoolLiquidityAmounts(
            amountA = TokenIndex.TokenAmount(idTokenA, null),
            amountB = TokenIndex.TokenAmount(idTokenB, null),
        )
    }

    suspend fun getPoolSwapResultFor(poolSwap: CustomTX.PoolSwap, blockHeight: Int, txn: Int): TokenIndex.TokenAmount? {
        if (poolSwap.toAddress == "") {
            return null
        }

        val amounts = getPoolSwapAmounts(poolSwap.fromAddress, blockHeight, txn).toMutableList()
        if (amounts.isEmpty()) {
            return null
        }

        val firstAmount = amounts.first()
        check(firstAmount.amount != null)

        if (poolSwap.fromToken != poolSwap.toToken) {
            check(
                firstAmount.amount < 0.0
                        && -firstAmount.amount == poolSwap.fromAmount
                        && firstAmount.tokenID == poolSwap.fromToken
            ) {
                err(
                    "firstAmount" to firstAmount,
                    "poolSwap" to poolSwap,
                )
            }
        }

        if (poolSwap.fromAddress != poolSwap.toAddress) {
            check(amounts.size == 1) {
                err("amounts" to amounts)
            }
            amounts += getPoolSwapAmounts(poolSwap.toAddress, blockHeight, txn)
            check(amounts.size <= 2) {
                err("amounts" to amounts)
            }
        }

        if (amounts.isEmpty()) {
            return null
        }

        if (amounts.size == 1) {
            if (poolSwap.fromToken == poolSwap.toToken) {
                return TokenIndex.TokenAmount(firstAmount.tokenID, poolSwap.fromAmount + firstAmount.amount)
            }
            return null
        }

        val lastAmount = amounts.last()
        check(lastAmount.amount != null)
        check(amounts.size == 2 && lastAmount.amount >= 0.0) {
            err(
                "amounts" to amounts,
                "lastAmount" to lastAmount,
                "poolSwap" to poolSwap
            )
        }
        return TokenIndex.TokenAmount(lastAmount.tokenID, lastAmount.amount)
    }

    private suspend fun getPoolSwapAmounts(owner: String, blockHeight: Int, txn: Int): List<TokenIndex.TokenAmount> {
        val swap = getRecord<PoolSwap>(owner, blockHeight, txn) ?: return emptyList()

        check(swap.type == "PoolSwap") { err("poolSwap" to swap) }

        val amounts = swap.amounts.map {
            TokenIndex.decodeTokenAmount(it)
        }.sortedBy { it.amount }

        check(amounts.size in 1..2) { err("amounts" to amounts) }
        return amounts
    }

    private suspend inline fun <reified T : Record> getRecord(
        owner: String,
        blockHeight: Int,
        txn: Int
    ): T? {
        val jsonRecord = RPC.getValue<JsonObject>(
            RPCMethod.GET_ACCOUNT_HISTORY,
            JsonPrimitive(owner),
            JsonPrimitive(blockHeight),
            JsonPrimitive(txn)
        )
        if (jsonRecord.isEmpty()) {
            return null
        }

        try {
            val record = decoder.decodeFromJsonElement<T>(jsonRecord)

            check(record.owner == owner) { err("record" to record, "owner" to owner) }
            check(record.blockHeight == blockHeight) { err("record" to record, "blockHeight" to blockHeight) }
            check(record.txn == txn) { err("record" to record, "txn" to txn) }

            return record
        } catch (e: SerializationException) {
            throw RuntimeException(
                "Invalid record $jsonRecord for (owner=$owner, blockHeight=$blockHeight, txn=$txn) ",
                e
            )
        }
    }

    data class PoolLiquidityAmounts(
        val amountA: TokenIndex.TokenAmount,
        val amountB: TokenIndex.TokenAmount,
    )

    interface Record {
        val type: String
        val owner: String
        val blockHeight: Int
        val txn: Int
    }

    @kotlinx.serialization.Serializable
    data class PoolSwap(
        val amounts: List<String>,
        override val type: String,
        override val owner: String,
        override val blockHeight: Int,
        override val txn: Int,
    ) : Record

    @kotlinx.serialization.Serializable
    data class PoolLiquidity(
        val amounts: List<String>,
        override val type: String,
        override val owner: String,
        override val blockHeight: Int,
        override val txn: Int,
    ) : Record
}