package com.trader.defichain.indexer

import com.trader.defichain.db.DBTX
import com.trader.defichain.db.insertTokens
import com.trader.defichain.dex.PoolPair
import com.trader.defichain.rpc.RPC
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("tokens")
private val allTokenSymbolsByTokenID = mutableMapOf<Int, String>()
private val poolPairs = mutableMapOf<Int, PoolPair>()
private val pooledTokenIdentifiersByTokenSymbol = mutableMapOf<String, Int>()

object TokenIndex {

    fun decodeTokenAmount(tokenAmount: String): TokenAmount {
        val (amount, symbol) = tokenAmount.split("@")
        return TokenAmount(
            amount = amount.toDouble(),
            tokenID = pooledTokenIdentifiersByTokenSymbol.getValue(symbol)
        )
    }

    fun getPoolPair(poolID: Int): PoolPair = poolPairs.getValue(poolID)

    fun getSymbol(tokenID: Int) = allTokenSymbolsByTokenID.getValue(tokenID)
    fun getPoolID(tokenA: Int, tokenB: Int): Int {
        val tokens = setOf(tokenA.toString(), tokenB.toString())
        return poolPairs.entries
            .first { tokens.contains(it.value.idTokenA) && tokens.contains(it.value.idTokenB) }
            .key
    }

    data class TokenAmount(
        val tokenID: Int,
        val amount: Double,
    )
}

suspend fun indexTokens(dbTX: DBTX) {
    poolPairs.putAll(RPC.listPoolPairs().entries.associate {
        it.key.toInt() to it.value
    })

    val tokenSymbolsByTokenID = RPC.listTokens().entries.associate {
        it.key.toInt() to it.value.symbol
    }

    val tokenChanges = mutableMapOf<Int, String>()
    for ((tokenID, tokenSymbol) in tokenSymbolsByTokenID) {
        val existingSymbol = allTokenSymbolsByTokenID[tokenID]
        if (tokenSymbol == existingSymbol) continue
        tokenChanges[tokenID] = tokenSymbol
    }

    allTokenSymbolsByTokenID.putAll(tokenSymbolsByTokenID)

    for ((poolID, pool) in poolPairs) {
        putPoolTokenIDBySymbol(poolID)
        putPoolTokenIDBySymbol(pool.idTokenA.toInt())
        putPoolTokenIDBySymbol(pool.idTokenB.toInt())
    }

    if (tokenChanges.isNotEmpty()) {
        logger.info("Indexing tokens $tokenChanges")
        dbTX.insertTokens(*tokenChanges.keys.toIntArray())
    }
}

private fun putPoolTokenIDBySymbol(tokenID: Int) {
    val tokenSymbol = allTokenSymbolsByTokenID.getValue(tokenID)
    val pooledTokenID = pooledTokenIdentifiersByTokenSymbol[tokenSymbol]
    check(pooledTokenID == null || pooledTokenID == tokenID)
    pooledTokenIdentifiersByTokenSymbol[tokenSymbol] = tokenID
}