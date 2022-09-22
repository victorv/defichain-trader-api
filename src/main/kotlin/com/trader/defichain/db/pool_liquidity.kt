package com.trader.defichain.db

import com.trader.defichain.indexer.TokenIndex
import com.trader.defichain.rpc.AccountHistory
import com.trader.defichain.rpc.CustomTX

private val template_insertPoolLiquidity = """
    INSERT INTO pool_liquidity (tx_row_id, token_a, token_b, amount_a, amount_b, pool, shares, owner) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO UPDATE SET
    token_a = ?,
    token_b = ?,
    amount_a = ?,
    amount_b = ?,
    pool = ?,
    shares = ?,
    owner = ?
    """.trimIndent()

fun DBTX.removePoolLiquidity(
    txRowID: Long,
    liquidity: CustomTX.RemovePoolLiquidity,
    amounts: AccountHistory.PoolLiquidityAmounts
) {
    val amountA = amounts.amountA
    val amountB = amounts.amountB

    insertTokens(amountA.tokenID, amountB.tokenID, liquidity.poolID)

    val ownerRowID = insertAddress(liquidity.owner)

    dbUpdater.prepareStatement(template_insertPoolLiquidity).use {
        it.setLong(1, txRowID)
        it.setInt(2, amountA.tokenID)
        it.setInt(3, amountB.tokenID)
        it.setDouble(4, amountA.amount)
        it.setDouble(5, amountB.amount)
        it.setInt(6, liquidity.poolID)
        it.setDouble(7, liquidity.shares)
        it.setLong(8, ownerRowID)

        it.setInt(9, amountA.tokenID)
        it.setInt(10, amountB.tokenID)
        it.setDouble(11, amountA.amount)
        it.setDouble(12, amountB.amount)
        it.setInt(13, liquidity.poolID)
        it.setDouble(14, liquidity.shares)
        it.setLong(15, ownerRowID)
        check(it.executeUpdate() <= 1)
    }
}

fun DBTX.addPoolLiquidity(txRowID: Long, liquidity: CustomTX.AddPoolLiquidity, shares: TokenIndex.TokenAmount) {
    insertTokens(liquidity.tokenA, liquidity.tokenB, shares.tokenID)

    val ownerRowID = insertAddress(liquidity.owner)

    dbUpdater.prepareStatement(template_insertPoolLiquidity).use {
        it.setLong(1, txRowID)
        it.setInt(2, liquidity.tokenA)
        it.setInt(3, liquidity.tokenB)
        it.setDouble(4, liquidity.amountA)
        it.setDouble(5, liquidity.amountB)
        it.setInt(6, shares.tokenID)
        it.setDouble(7, shares.amount)
        it.setLong(8, ownerRowID)

        it.setInt(9, liquidity.tokenA)
        it.setInt(10, liquidity.tokenB)
        it.setDouble(11, liquidity.amountA)
        it.setDouble(12, liquidity.amountB)
        it.setInt(13, shares.tokenID)
        it.setDouble(14, shares.amount)
        it.setLong(15, ownerRowID)
        check(it.executeUpdate() <= 1)
    }
}