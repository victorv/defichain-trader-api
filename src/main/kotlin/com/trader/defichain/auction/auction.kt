package com.trader.defichain.auction

import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.SwapResult
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.rpc.RPC
import com.trader.defichain.rpc.RPCMethod

fun parseTokenAmount(text: String): TokenAmount {
    val (amount, token) = text.split("@")
    val value = amount.toDouble()
    val usdtValue = if (value == 0.0) 0.0 else testPoolSwap(
        PoolSwap(
            amountFrom = value,
            tokenFrom = token,
            tokenTo = "USDT",
            desiredResult = 1.0,
        )
    ).estimate
    return TokenAmount(
        value = value,
        valueUSD = usdtValue,
        token = token
    )
}

suspend fun listAuctions(): ArrayList<Auction> {
    val rpcAuctions = RPC.listAuctions()
    val auctions = ArrayList<Auction>()
    for (rpcAuction in rpcAuctions) {
        val batches = ArrayList<AuctionBatch>()
        for (rpcBatch in rpcAuction.batches) {

            val loan = parseTokenAmount(rpcBatch.loan)
            var minimumBid = loan.value * (1.0 + rpcAuction.liquidationPenalty / 100.0)
            var highestBid: AuctionBid? = null
            if (rpcBatch.highestBid != null) {
                val bidAmount = parseTokenAmount(rpcBatch.highestBid.amount)
                highestBid = AuctionBid(
                    amount = bidAmount,
                    owner = rpcBatch.highestBid.owner,
                )
                minimumBid = bidAmount.value * 1.0101
            }

            val collaterals = ArrayList<Collateral>()
            for (rpcCollateral in rpcBatch.collaterals) {
                val tokenAmount = parseTokenAmount(rpcCollateral)
                val toLoan =
                    if (tokenAmount.value == 0.0) null
                    else if (tokenAmount.token == loan.token) null
                    else testPoolSwap(
                        PoolSwap(
                            amountFrom = tokenAmount.value,
                            tokenFrom = tokenAmount.token,
                            tokenTo = loan.token,
                            desiredResult = minimumBid,
                        )
                    )

                val fromLoan = if (tokenAmount.value == 0.0) null
                else if (tokenAmount.token == loan.token) null
                else testPoolSwap(
                    PoolSwap(
                        amountFrom = loan.value,
                        tokenFrom = loan.token,
                        tokenTo = tokenAmount.token,
                        desiredResult = tokenAmount.value,
                    )
                )

                val collateral = Collateral(
                    tokenAmount = tokenAmount,
                    swapFromLoan = fromLoan,
                    swapToLoan = toLoan,
                )
                collaterals.add(collateral)
            }

            val maximumBid = collaterals.sumOf { it.swapToLoan?.estimate ?: 0.0 }

            val batch = AuctionBatch(
                index = rpcBatch.index,
                loan = loan,
                collaterals = collaterals,
                highestBid = highestBid,
                minimumBid = minimumBid,
                maximumBid = maximumBid
            )
            batches.add(batch)
        }

        val auction = Auction(
            vaultId = rpcAuction.vaultId,
            ownerAddress = rpcAuction.ownerAddress,
            liquidationHeight = rpcAuction.liquidationHeight,
            batchCount = rpcAuction.batchCount,
            batches = batches
        )
        auctions.add(auction)
    }

    val blockCount = RPC.getValue<Int>(RPCMethod.GET_BLOCK_COUNT)
    for (auction in auctions) {
        auction.blocksRemaining = auction.liquidationHeight - blockCount
    }

    return auctions
}

@kotlinx.serialization.Serializable
data class TokenAmount(
    val value: Double,
    val token: String,
    val valueUSD: Double,
)

@kotlinx.serialization.Serializable
data class Collateral(
    val tokenAmount: TokenAmount,
    val swapToLoan: SwapResult?,
    val swapFromLoan: SwapResult?,
)

@kotlinx.serialization.Serializable
data class AuctionBid(
    val owner: String,
    val amount: TokenAmount,
)

@kotlinx.serialization.Serializable
data class AuctionBatch(
    val index: Int,
    val collaterals: List<Collateral>,
    val loan: TokenAmount,
    val highestBid: AuctionBid?,
    val minimumBid: Double,
    val maximumBid: Double,
)

@kotlinx.serialization.Serializable
data class Auction(
    val vaultId: String,
    val ownerAddress: String,
    val liquidationHeight: Int,
    val batchCount: Int,
    val batches: List<AuctionBatch>,
    var blocksRemaining: Int = 0
)