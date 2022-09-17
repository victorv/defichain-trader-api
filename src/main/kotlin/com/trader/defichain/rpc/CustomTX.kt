package com.trader.defichain.rpc

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive

private val numberMatcher = "^\\d+$".toRegex()

object CustomTX {

    data class PoolSwap(
        val fromAddress: String,
        val toAddress: String,
        val fromToken: Int,
        val toToken: Int,
        val fromAmount: Double,
        val maxPrice: Double,
        var amountTo: Double? = null,
    )

    data class AddPoolLiquidity(
        val amountA: Double,
        val amountB: Double,
        val tokenA: Int,
        val tokenB: Int,
        val owner: String,
    )

    data class RemovePoolLiquidity(
        val poolID: Int,
        val shares: Double,
        val owner: String,
    )

    @kotlinx.serialization.Serializable
    data class Record(
        val type: String,
        val valid: Boolean,
        val results: JsonObject
    ) {
        fun isPoolSwap() = type == "PoolSwap"
        fun isAddPoolLiquidity() = type == "AddPoolLiquidity"
        fun isRemovePoolLiquidity() = type == "RemovePoolLiquidity"

        fun asPoolSwap() = PoolSwap(
            fromAddress = results.getValue("fromAddress").jsonPrimitive.content,
            toAddress = results.getValue("toAddress").jsonPrimitive.content,
            fromToken = results.getValue("fromToken").jsonPrimitive.int,
            toToken = results.getValue("toToken").jsonPrimitive.int,
            fromAmount = results.getValue("fromAmount").jsonPrimitive.double,
            maxPrice = results.getValue("maxPrice").jsonPrimitive.double,
        )

        fun asAddPoolLiquidity(): AddPoolLiquidity {
            val owner = results.getValue("shareaddress").jsonPrimitive.content
            val amounts = results.entries
                .filter { numberMatcher.matches(it.key) }
                .map { Pair(it.key.toInt(), it.value.jsonPrimitive.double) }
                .sortedBy { it.first }


            check(amounts.size == 2 && amounts.all { it.second > 0.0 })

            val (tokenA, amountA) = amounts.first()
            val (tokenB, amountB) = amounts.last()
            check(amountA > 0.0)
            check(amountB > 0.0)
            check(tokenA != tokenB)

            return AddPoolLiquidity(
                owner = owner,
                amountA = -amountA,
                amountB = -amountB,
                tokenA = tokenA,
                tokenB = tokenB,
            )
        }

        fun asRemovePoolLiquidity(): RemovePoolLiquidity {
            val owner = results.getValue("from").jsonPrimitive.content
            val (amount, poolID) = results.getValue("amount").jsonPrimitive.content.split("@")
            val nAmount = amount.toDouble()
            check(nAmount > 0.0)
            return RemovePoolLiquidity(
                owner = owner,
                poolID = poolID.toInt(),
                shares = -nAmount,
            )
        }
    }
}