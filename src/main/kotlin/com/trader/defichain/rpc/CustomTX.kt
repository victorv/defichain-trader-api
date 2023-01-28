package com.trader.defichain.rpc

import com.trader.defichain.indexer.TokenIndex
import kotlinx.serialization.json.*

private val numberMatcher = "^\\d+$".toRegex()

private fun decodeAmount(amount: String): Pair<Double, Int> {
    val (amountString, tokenIDString) = amount.split("@")
    return Pair(amountString.toDouble(), tokenIDString.toInt())
}

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
        fun isTakeLoan() = type == "TakeLoan"
        fun isPaybackLoan() = type == "PaybackLoan"
        fun isAuctionBid() = type == "AuctionBid"
        fun isAnyAccountsToAccounts() = type == "AnyAccountsToAccounts"
        fun isAccountToAccount() = type == "AccountToAccount"
        fun isAccountToUtxos() = type == "AccountToUtxos"
        fun isUtxosToAccount() = type == "UtxosToAccount"
        fun isSetOracleData() = type == "SetOracleData"

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

        private fun getAddressAmounts(results: JsonObject): List<Pair<Double, Int>> {
            return results.entries.map {
                val (amount, tokenID) = it.value.jsonPrimitive.content.split("@")
                amount.toDouble() to tokenID.toInt()
            }
        }

        private fun getAmounts(results: JsonObject): List<Pair<Double, Int>> {
            if(results.isEmpty()) {
                return emptyList()
            }

            if (results.containsKey("amount")) {
                val amount = results.getValue("amount").jsonPrimitive.content
                return listOf(decodeAmount(amount))
            }

            if (results.containsKey("dToken")) {
                val amountEntries = results.getValue("dToken").jsonArray
                val amounts = ArrayList<Pair<Double, Int>>()
                for (entry in amountEntries) {
                    val entryObject = entry as JsonObject
                    val dTokens = entryObject.getValue("dTokens").jsonPrimitive.content
                    val amount = entryObject.getValue(dTokens).jsonPrimitive.content
                    amounts.add(Pair(amount.toDouble(), dTokens.toInt()))
                }
                return amounts
            }

            val amount = results.entries.firstOrNull { numberMatcher.matches(it.key) } ?: return emptyList()
            return listOf(Pair(amount.value.jsonPrimitive.content.toDouble(), amount.key.toInt()))
        }

        fun asDepositToVault() = DepositToVault(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            from = results.getValue("from").jsonPrimitive.content,
            amounts = getAmounts(results).map { Pair(-it.first, it.second) },
        )

        fun asWithdrawFromVault() = WithdrawFromVault(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            to = results.getOrDefault("to", JsonPrimitive("")).jsonPrimitive.content,
            amounts = getAmounts(results),
        )

        fun asTakeLoan() = TakeLoan(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            to = results.getOrDefault("to", JsonPrimitive("")).jsonPrimitive.content,
            amounts = getAmounts(results),
        )

        fun asPaybackLoan() = PaybackLoan(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            from = results.getValue("from").jsonPrimitive.content,
            amounts = getAmounts(results).map { Pair(-it.first, it.second) }
        )

        fun asAuctionBid() = AuctionBid(
            vaultID = results.getValue("vaultId").jsonPrimitive.content,
            from = results.getValue("from").jsonPrimitive.content,
            amount = results.getValue("amount").jsonPrimitive.content,
            index = results.getValue("index").jsonPrimitive.int
        )

        fun asSetOracleData() = results.getValue("tokenPrices").jsonArray.size

        fun asAnyAccountsToAccounts(): List<Pair<Double, Int>> = getAddressAmounts(results.getValue("from").jsonObject)

        fun asAccountToAccount(): List<Pair<Double, Int>> = getAddressAmounts(results.getValue("to").jsonObject)
        fun asUtxosToAccount(): List<Pair<Double, Int>> = getAddressAmounts(results)
        fun asAccountToUtxos(): List<Pair<Double, Int>> = getAddressAmounts(results.getValue("to").jsonObject)
    }

    data class PoolSwap(
        val fromAddress: String,
        val toAddress: String,
        val fromToken: Int,
        val toToken: Int,
        val fromAmount: Double,
        val maxPrice: Double,
        var amountTo: TokenIndex.TokenAmount? = null,
        var path: Int = 0
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

    interface CollateralOrLoan {
        val vaultID: String
        val amounts: List<Pair<Double, Int>>
        fun owner(): String
    }

    data class DepositToVault(
        override val vaultID: String,
        override val amounts: List<Pair<Double, Int>>,
        val from: String,
    ) : CollateralOrLoan {

        override fun owner() = from
    }

    data class WithdrawFromVault(
        override val vaultID: String,
        override val amounts: List<Pair<Double, Int>>,
        val to: String,
    ) : CollateralOrLoan {

        override fun owner() = to
    }

    data class TakeLoan(
        override val vaultID: String,
        override val amounts: List<Pair<Double, Int>>,
        val to: String,
    ) : CollateralOrLoan {

        override fun owner() = to
    }

    data class PaybackLoan(
        override val vaultID: String,
        override val amounts: List<Pair<Double, Int>>,
        val from: String,
    ) : CollateralOrLoan {

        override fun owner() = from
    }

    data class AuctionBid(
        val vaultID: String,
        val amount: String,
        val from: String,
        val index: Int,
    ) {
        fun amount(): Pair<Double, Int> {
            return decodeAmount(amount)
        }
    }
}