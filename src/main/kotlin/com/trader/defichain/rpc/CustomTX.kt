package com.trader.defichain.rpc

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive

private val numberMatcher = "^\\d+$".toRegex()

object CustomTX {

    @kotlinx.serialization.Serializable
    data class Record(
        val type: String,
        val valid: Boolean,
        val results: JsonObject
    ) {
        fun isPoolSwap() = type == "PoolSwap"
        fun isAddPoolLiquidity() = type == "AddPoolLiquidity"
        fun isRemovePoolLiquidity() = type == "RemovePoolLiquidity"
        fun isDepositToVault() = type == "DepositToVault"
        fun isWithdrawFromVault() = type == "WithdrawFromVault"

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

        fun asDepositToVault() = DepositToVault(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            from = results.getValue("from").jsonPrimitive.content,
            amount = results.getValue("amount").jsonPrimitive.content,
        )

        fun asWithdrawFromVault() = WithdrawFromVault(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            to = results.getValue("to").jsonPrimitive.content,
            amount = results.getValue("amount").jsonPrimitive.content,
        )
    }

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

    interface Collateral {
        val vaultID: String

        fun amount(): Pair<Double, Int>
        fun owner(): String
    }

    data class DepositToVault(
        override val vaultID: String,
        private val amount: String,
        val from: String,
    ) : Collateral {

        override fun amount(): Pair<Double, Int> {
            val (amountString, tokenIDString) = amount.split("@")
            return Pair(amountString.toDouble(), tokenIDString.toInt())
        }
        override fun owner() = from
    }

    data class WithdrawFromVault(
        override val vaultID: String,
        private val amount: String,
        val to: String,
    ) : Collateral {

        override fun amount(): Pair<Double, Int> {
            val (amountString, tokenIDString) = amount.split("@")
            return Pair(-amountString.toDouble(), tokenIDString.toInt())
        }
        override fun owner() = to
    }
}