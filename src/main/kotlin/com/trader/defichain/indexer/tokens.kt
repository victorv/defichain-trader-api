package com.trader.defichain.indexer

import com.trader.defichain.db.DBTX
import com.trader.defichain.db.insertTokens
import com.trader.defichain.dex.getPools
import com.trader.defichain.dex.getTokens
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("tokens")
private val allTokenSymbolsByTokenID = mutableMapOf<Int, String>()
private val pooledTokenIdentifiersByTokenSymbol = mutableMapOf<String, Int>()

object TokenIndex {

    fun decodeTokenAmount(tokenAmount: String): TokenAmount {
        val (amount, symbol) = tokenAmount.split("@")
        return TokenAmount(
            amount = amount.toDouble(),
            tokenID = pooledTokenIdentifiersByTokenSymbol.getValue(symbol)
        )
    }

    fun getSymbol(tokenID: Int) = allTokenSymbolsByTokenID.getValue(tokenID)

    data class TokenAmount(
        val tokenID: Int,
        val amount: Double?,
    )
}

fun DBTX.indexTokens() {

    val tokensByID = getTokens()

    val tokenChanges = mutableMapOf<Int, String>()
    for ((tokenID, token) in tokensByID) {
        val existingSymbol = allTokenSymbolsByTokenID[tokenID]
        if (token.symbol == existingSymbol) continue
        tokenChanges[tokenID] = token.symbol
    }

    allTokenSymbolsByTokenID.putAll(tokensByID.entries.associate { it.key to it.value.symbol })

    for ((poolID, pool) in getPools()) {
        putPoolTokenIDBySymbol(poolID)
        putPoolTokenIDBySymbol(pool.idTokenA)
        putPoolTokenIDBySymbol(pool.idTokenB)
    }

    if (tokenChanges.isNotEmpty()) {
        doLater {
            logger.info("Indexing tokens $tokenChanges")
            insertTokens(*tokenChanges.keys.toTypedArray())
        }
    }
}

private fun putPoolTokenIDBySymbol(tokenID: Int) {
    val tokenSymbol = allTokenSymbolsByTokenID.getValue(tokenID)
    val pooledTokenID = pooledTokenIdentifiersByTokenSymbol[tokenSymbol]
    check(pooledTokenID == null || pooledTokenID == tokenID)
    pooledTokenIdentifiersByTokenSymbol[tokenSymbol] = tokenID
}