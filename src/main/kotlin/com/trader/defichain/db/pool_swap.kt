package com.trader.defichain.db

import com.trader.defichain.rpc.CustomTX
import kotlin.math.absoluteValue

private val template_insertPoolSwap = """
    INSERT INTO pool_swap (tx_row_id, "from", "to", token_from, token_to, amount_from, amount_to, max_price) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO UPDATE set amount_to = coalesce(?, pool_swap.amount_to)
    """.trimIndent()

fun DBTX.insertPoolSwap(txRowID: Long, swap: CustomTX.PoolSwap) {
    insertTokens(swap.fromToken)
    insertTokens(swap.toToken)

    val fromRowID = insertAddress(swap.fromAddress)
    val toRowID = insertAddress(swap.toAddress)

    dbUpdater.prepareStatement(template_insertPoolSwap).use {
        it.setLong(1, txRowID)
        it.setLong(2, fromRowID)
        it.setLong(3, toRowID)
        it.setInt(4, swap.fromToken)
        it.setInt(5, swap.toToken)
        it.setDouble(6, swap.fromAmount.absoluteValue)
        it.setDoubleOrNull(7, swap.amountTo)
        it.setDouble(8, swap.maxPrice)
        it.setDoubleOrNull(9, swap.amountTo)
        check(it.executeUpdate() == 1)
    }
}