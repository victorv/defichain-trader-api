package com.trader.defichain.db

import com.trader.defichain.indexer.ZMQRawTX
import com.trader.defichain.rpc.Block

private const val template_insertBlock = """
INSERT INTO block (height, time, hash) VALUES (?, ?, ?)
ON CONFLICT (height) DO NOTHING
"""

private const val template_insertMempoolTX = """
INSERT INTO mempool (tx_row_id, time, block_height, txn) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_row_id) DO NOTHING
"""

private const val template_insertMintedTX = """
INSERT INTO minted_tx (tx_row_id, block_height, txn, fee) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_row_id) DO UPDATE SET tx_row_id = minted_tx.tx_row_id
"""

private const val template_insertTXType = """
INSERT INTO tx_type (dc_tx_type) VALUES (?)
ON CONFLICT (dc_tx_type) DO UPDATE SET dc_tx_type = tx_type.dc_tx_type
RETURNING row_id;
"""

private const val template_insertTX = """
INSERT INTO tx (dc_tx_id, tx_type_row_id) VALUES (?, ?)
ON CONFLICT (dc_tx_id) DO UPDATE SET dc_tx_id = tx.dc_tx_id
RETURNING row_id;
"""

fun DBTX.insertBlock(block: Block) {
    dbUpdater.prepareStatement(template_insertBlock).use {
        it.setLong(1, block.height)
        it.setLong(2, block.time)
        it.setString(3, block.hash)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertRawTX(txRowID: Long, tx: ZMQRawTX) {
    dbUpdater.prepareStatement(template_insertMempoolTX).use {
        it.setLong(1, txRowID)
        it.setLong(2, tx.time)
        it.setLong(3, tx.blockHeight)
        it.setInt(4, tx.txn)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertMintedTX(txRowID: Long, mintedTX: DB.MintedTX) {
    dbUpdater.prepareStatement(template_insertMintedTX).use {
        it.setLong(1, txRowID)
        it.setLong(2, mintedTX.blockHeight)
        it.setInt(3, mintedTX.txn)
        it.setBigDecimal(4, mintedTX.txFee)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertTX(txID: String, txType: String): Long {
    val txTypeRowID = insertTransactionType(txType)

    dbUpdater.prepareStatement(template_insertTX).use {
        it.setString(1, txID)
        it.setInt(2, txTypeRowID)
        return upsertReturning(it)
    }
}

private fun DBTX.insertTransactionType(txType: String): Int {
    dbUpdater.prepareStatement(template_insertTXType).use {
        it.setString(1, txType)
        return upsertReturning(it).toInt()
    }
}