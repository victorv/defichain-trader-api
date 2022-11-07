package com.trader.defichain.db

import com.trader.defichain.dex.PoolPair
import com.trader.defichain.indexer.indexTokens
import com.trader.defichain.rpc.Block
import com.trader.defichain.util.Future

private val template_insertPoolPair = """
INSERT INTO pool_pair (reserve_a, reserve_b, token, block_height) 
VALUES (?, ?, ?, ?)
ON CONFLICT(token, block_height) DO UPDATE SET
reserve_a = ?,
reserve_b = ?
""".trimIndent()

private val template_insertFee = """
INSERT INTO fee (in_a, out_a, in_b, out_b, commission, token, block_height) 
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(token, block_height) DO NOTHING
""".trimIndent()

private val template_insertOraclePrice = """
INSERT INTO oracle_price (price, token, block_height) 
VALUES (?, ?, ?)
ON CONFLICT(token, block_height) DO NOTHING
""".trimIndent()

private val oldPoolPairs = mutableMapOf<Int, PoolPair>()
private val oldFees = mutableMapOf<Int, Fee>()
private val oldOraclePrices = mutableMapOf<Int, Double>()

fun updatePoolPair(poolID: Int, poolPair: PoolPair): Boolean {
    val oldPoolPair = oldPoolPairs[poolID]
    if (oldPoolPair == poolPair) return false
    oldPoolPairs[poolID] = poolPair
    return true
}

fun updateFee(poolID: Int, poolPair: PoolPair): Boolean {
    val fee = Fee(
        dexFeeInPctTokenA = poolPair.dexFeeInPctTokenA,
        dexFeeOutPctTokenA = poolPair.dexFeeOutPctTokenA,
        dexFeeInPctTokenB = poolPair.dexFeeInPctTokenB,
        dexFeeOutPctTokenB = poolPair.dexFeeInPctTokenB,
        commission = poolPair.commission,
    )
    val oldFee = oldFees[poolID]
    if (oldFee == fee) return false
    oldFees[poolID] = fee
    return true
}

fun updateOraclePrice(tokenID: Int, oraclePrice: Double): Boolean {
    val oldOraclePrice = oldOraclePrices[tokenID]
    if (oldOraclePrice == oraclePrice) return false
    oldOraclePrices[tokenID] = oraclePrice
    return true
}

fun DBTX.insertPoolPairs(block: Block, masterNode: Future<Long>, poolPairs: Map<Int, PoolPair>, oraclePrices: Map<Int, Double>) {
    insertBlock(block, masterNode, false)

    doLater {
        indexTokens()

        connection.prepareStatement(template_insertPoolPair).use {
            for ((poolID, poolPair) in poolPairs) {
                if (updatePoolPair(poolID, poolPair)) {
                    it.setDouble(1, poolPair.reserveA)
                    it.setDouble(2, poolPair.reserveB)
                    it.setInt(3, poolID)
                    it.setInt(4, block.height)
                    it.setDouble(5, poolPair.reserveA)
                    it.setDouble(6, poolPair.reserveB)
                    it.addBatch()
                }
            }
            it.executeBatch()
        }

        connection.prepareStatement(template_insertFee).use {
            for ((poolID, poolPair) in poolPairs) {
                if (updateFee(poolID, poolPair)) {
                    it.setObject(1, poolPair.dexFeeInPctTokenA)
                    it.setObject(2, poolPair.dexFeeOutPctTokenA)
                    it.setObject(3, poolPair.dexFeeInPctTokenB)
                    it.setObject(4, poolPair.dexFeeInPctTokenB)
                    it.setObject(5, poolPair.commission)
                    it.setInt(6, poolID)
                    it.setInt(7, block.height)
                    it.addBatch()
                }
            }
            it.executeBatch()
        }

        connection.prepareStatement(template_insertOraclePrice).use {
            for ((tokenID, price) in oraclePrices) {
                if (updateOraclePrice(tokenID, price)) {
                    it.setDouble(1, price)
                    it.setInt(2, tokenID)
                    it.setInt(3, block.height)
                    it.addBatch()
                }
            }
            it.executeBatch()
        }
    }
}

data class Fee(
    val dexFeePctTokenA: Double? = null,
    val dexFeeInPctTokenA: Double? = null,
    val dexFeeOutPctTokenA: Double? = null,
    val dexFeePctTokenB: Double? = null,
    val dexFeeOutPctTokenB: Double? = null,
    val dexFeeInPctTokenB: Double? = null,
    val commission: Double? = null,
)