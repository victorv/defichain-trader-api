package com.trader.defichain.indexer

import com.trader.defichain.db.DB
import com.trader.defichain.rpc.RPC
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("tokens")
private val allTokenSymbolsByTokenID = mutableMapOf<Int, String>()

suspend fun indexTokens(dbUpdater: DB.Updater) {
    val tokenSymbolsByTokenID = RPC.listTokens().entries.associate {
        it.key.toInt() to it.value.symbol
    }

    val tokenChanges = mutableMapOf<Int, String>()
    for ((tokenID, tokenSymbol) in tokenSymbolsByTokenID) {
        val existingSymbol = allTokenSymbolsByTokenID[tokenID]
        if (tokenSymbol == existingSymbol) continue
        tokenChanges[tokenID] = tokenSymbol
    }

    if (tokenChanges.isNotEmpty()) {
        logger.info("Indexing tokens $tokenChanges")
        DB.insertTokens(dbUpdater, tokenChanges)
    }

    allTokenSymbolsByTokenID.putAll(tokenSymbolsByTokenID)
}