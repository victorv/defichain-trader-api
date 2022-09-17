package com.trader.defichain.db

private const val template_insertToken = """
INSERT INTO token (dc_token_id, dc_token_symbol) VALUES (?, ?)
ON CONFLICT(dc_token_id) DO UPDATE set dc_token_symbol = ?
"""

private val tokens = mutableMapOf<Int, String>()

fun DBTX.insertTokens(tokens: Map<Int, String>) {
    if (tokens.isEmpty()) return

    for ((tokenID, tokenSymbol) in tokens) {
        insertToken(tokenID, tokenSymbol)
    }
}

fun DBTX.insertToken(tokenID: Int, tokenSymbol: String) {
    val currentTokenSymbol = tokens[tokenID]
    if (tokenSymbol == currentTokenSymbol) {
        return
    }

    dbUpdater.prepareStatement(template_insertToken).use {
        it.setInt(1, tokenID)
        it.setString(2, tokenSymbol)
        it.setString(3, tokenSymbol)
        check(it.executeUpdate() <= 1)

        tokens[tokenID] = tokenSymbol
    }
}