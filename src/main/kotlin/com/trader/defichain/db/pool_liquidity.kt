package com.trader.defichain.db

import com.trader.defichain.indexer.TokenIndex
import com.trader.defichain.rpc.CustomTX

private val template_insertPoolLiquidity = """
    INSERT INTO pool_liquidity (tx_row_id, token_a, token_b, amount_a, amount_b, pool, shares, owner) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO NOTHING
    """.trimIndent()

fun DBTX.insertPoolLiquidity(txRowID: Long, liquidity: CustomTX.AddPoolLiquidity, shares: TokenIndex.TokenAmount) {
    insertToken(liquidity.tokenA, TokenIndex.getSymbol(liquidity.tokenA))
    insertToken(liquidity.tokenB, TokenIndex.getSymbol(liquidity.tokenB))
    insertToken(shares.tokenID, TokenIndex.getSymbol(shares.tokenID))

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
        check(it.executeUpdate() == 1)
    }
}