package com.trader.defichain.db

import com.trader.defichain.indexer.TokenIndex

private const val template_deleteTokenAmounts = """
DELETE from token_amount where tx_row_id = ?
"""

private const val template_insertTokenAmount = """
INSERT INTO token_amount (tx_row_id, token, amount) VALUES (?, ?, ?)
"""

private const val template_insertToken = """
INSERT INTO token (dc_token_id, dc_token_symbol) VALUES (?, ?)
ON CONFLICT(dc_token_id) DO UPDATE set dc_token_symbol = ?
"""

private val tokens = mutableMapOf<Int, String>()

fun DBTX.insertTokenAmounts(txRowID: Long, amounts: List<Pair<Double, Int>>) {
    insertTokens(*amounts.map { it.second }.toTypedArray())

    connection.prepareStatement(template_deleteTokenAmounts).use {
        it.setLong(1, txRowID)
        it.executeUpdate()
    }

    connection.prepareStatement(template_insertTokenAmount).use {
        for (amount in amounts) {
            it.setLong(1, txRowID)
            it.setInt(2, amount.second)
            it.setDouble(3, amount.first)
            it.addBatch()
        }
        check(it.executeBatch().all { count -> count == 1 })
    }
}

fun DBTX.insertTokens(vararg tokenIdentifiers: Int?) {
    for (tokenID in tokenIdentifiers) {
        if (tokenID == null) continue

        val tokenSymbol = TokenIndex.getSymbol(tokenID)
        val currentTokenSymbol = tokens[tokenID]
        if (tokenSymbol == currentTokenSymbol) {
            continue
        }

        connection.prepareStatement(template_insertToken).use {
            it.setInt(1, tokenID)
            it.setString(2, tokenSymbol)
            it.setString(3, tokenSymbol)
            check(it.executeUpdate() <= 1)

            tokens[tokenID] = tokenSymbol
        }
    }
}