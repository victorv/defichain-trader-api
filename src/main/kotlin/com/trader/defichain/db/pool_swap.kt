package com.trader.defichain.db

import com.trader.defichain.rpc.CustomTX
import com.trader.defichain.util.Future
import kotlin.math.absoluteValue

private val template_insertPoolSwap = """
    INSERT INTO pool_swap (tx_row_id, "from", "to", token_from, token_to, token_to_alt, amount_from, amount_to, max_price) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO UPDATE SET 
    "from" = ?,
    "to" = ?,
    token_from = ?,
    token_to = ?,
    token_to_alt = ?,
    amount_from = ?,
    amount_to = ?,
    max_price = ?
    """.trimIndent()

fun DBTX.insertPoolSwap(txRowID: Future<Long>, swap: CustomTX.PoolSwap) = doLater {
    insertTokens(swap.fromToken)
    insertTokens(swap.toToken)

    val fromRowID = insertAddress(swap.fromAddress)
    val toRowID = insertAddress(swap.toAddress)

    var amountTo = 0.0
    var tokenToAlt = swap.toToken
    val tokenAmount = swap.amountTo
    if (tokenAmount != null) {
        amountTo = tokenAmount.amount ?: 0.0
        tokenToAlt = tokenAmount.tokenID
    }

    connection.prepareStatement(template_insertPoolSwap).use {
        it.setLong(1, txRowID.get())
        it.setLong(2, fromRowID)
        it.setLong(3, toRowID)
        it.setInt(4, swap.fromToken)
        it.setInt(5, swap.toToken)
        it.setInt(6, tokenToAlt)
        it.setDouble(7, swap.fromAmount.absoluteValue)
        it.setDouble(8, amountTo)
        it.setDouble(9, swap.maxPrice)

        it.setLong(10, fromRowID)
        it.setLong(11, toRowID)
        it.setInt(12, swap.fromToken)
        it.setInt(13, swap.toToken)
        it.setInt(14, tokenToAlt)
        it.setDouble(15, swap.fromAmount.absoluteValue)
        it.setDouble(16, amountTo)
        it.setDouble(17, swap.maxPrice)

        check(it.executeUpdate() == 1)
    }
}