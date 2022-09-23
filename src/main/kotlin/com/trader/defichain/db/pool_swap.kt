package com.trader.defichain.db

import com.trader.defichain.rpc.CustomTX
import com.trader.defichain.util.Future
import kotlin.math.absoluteValue

private val template_insertPoolSwap = """
    INSERT INTO pool_swap (tx_row_id, "from", "to", token_from, token_to, amount_from, amount_to, max_price) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO UPDATE SET 
    "from" = ?,
    "to" = ?,
    token_from = ?,
    token_to = ?,
    amount_from = ?,
    amount_to = ?,
    max_price = ?
    """.trimIndent()

fun DBTX.insertPoolSwap(txRowID: Future<Long>, swap: CustomTX.PoolSwap) = doLater {
    insertTokens(swap.fromToken)
    insertTokens(swap.toToken)

    val fromRowID = insertAddress(swap.fromAddress)
    val toRowID = insertAddress(swap.toAddress)

    connection.prepareStatement(template_insertPoolSwap).use {
        it.setLong(1, txRowID.get())
        it.setLong(2, fromRowID)
        it.setLong(3, toRowID)
        it.setInt(4, swap.fromToken)
        it.setInt(5, swap.toToken)
        it.setDouble(6, swap.fromAmount.absoluteValue)
        it.setDoubleOrNull(7, swap.amountTo)
        it.setDouble(8, swap.maxPrice)

        it.setLong(9, fromRowID)
        it.setLong(10, toRowID)
        it.setInt(11, swap.fromToken)
        it.setInt(12, swap.toToken)
        it.setDouble(13, swap.fromAmount.absoluteValue)
        it.setDoubleOrNull(14, swap.amountTo)
        it.setDouble(15, swap.maxPrice)

        check(it.executeUpdate() == 1)
    }
}