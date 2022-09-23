package com.trader.defichain.db

import com.trader.defichain.indexer.TokenIndex

private const val template_insertToken = """
INSERT INTO token (dc_token_id, dc_token_symbol) VALUES (?, ?)
ON CONFLICT(dc_token_id) DO UPDATE set dc_token_symbol = ?
"""

private val tokens = mutableMapOf<Int, String>()

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