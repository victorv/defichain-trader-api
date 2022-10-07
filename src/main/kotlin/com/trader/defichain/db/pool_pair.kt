package com.trader.defichain.db

import com.trader.defichain.dex.PoolPair
import com.trader.defichain.indexer.indexTokens
import com.trader.defichain.rpc.Block

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

fun DBTX.insertPoolPairs(block: Block, poolPairs: Map<Int, PoolPair>) {
    insertBlock(block)

    doLater {
        indexTokens()

        connection.prepareStatement(template_insertPoolPair).use {
            for ((poolID, poolPair) in poolPairs) {
                it.setDouble(1, poolPair.reserveA)
                it.setDouble(2, poolPair.reserveB)
                it.setInt(3, poolID)
                it.setInt(4, block.height)
                it.setDouble(5, poolPair.reserveA)
                it.setDouble(6, poolPair.reserveB)
                it.addBatch()
            }
            it.executeBatch()
        }

        connection.prepareStatement(template_insertFee).use {
            for ((poolID, poolPair) in poolPairs) {
                it.setObject(1, poolPair.dexFeeInPctTokenA)
                it.setObject(2, poolPair.dexFeeOutPctTokenA)
                it.setObject(3, poolPair.dexFeeInPctTokenB)
                it.setObject(4, poolPair.dexFeeInPctTokenB)
                it.setObject(5, poolPair.commission)
                it.setInt(6, poolID)
                it.setInt(7, block.height)
                it.addBatch()
            }
            it.executeBatch()
        }
    }
}