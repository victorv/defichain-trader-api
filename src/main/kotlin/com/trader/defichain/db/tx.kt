package com.trader.defichain.db

import com.trader.defichain.indexer.ZMQRawTX
import com.trader.defichain.rpc.Block
import com.trader.defichain.util.Future
import java.math.BigDecimal

private const val template_insertBlock = """
INSERT INTO block (height, time, hash) VALUES (?, ?, ?)
ON CONFLICT (height) DO NOTHING
"""

private const val template_insertMempoolTX = """
INSERT INTO mempool (tx_row_id, time, block_height, txn) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_row_id) DO NOTHING
"""

private const val template_insertMintedTX = """
INSERT INTO minted_tx (tx_row_id, block_height, txn) VALUES (?, ?, ?)
ON CONFLICT (tx_row_id) DO UPDATE SET tx_row_id = minted_tx.tx_row_id
"""

private const val template_insertTXType = """
INSERT INTO tx_type (dc_tx_type) VALUES (?)
ON CONFLICT (dc_tx_type) DO UPDATE SET dc_tx_type = tx_type.dc_tx_type
RETURNING row_id;
"""

private const val template_insertTX = """
INSERT INTO tx (dc_tx_id, tx_type_row_id, fee, confirmed) VALUES (?, ?, ?, ?)
ON CONFLICT (dc_tx_id) DO UPDATE SET 
fee = ?,
confirmed = ?
RETURNING row_id;
"""

fun DBTX.insertBlock(block: Block) = doLater {
    connection.prepareStatement(template_insertBlock).use {
        it.setLong(1, block.height)
        it.setLong(2, block.time)
        it.setString(3, block.hash)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertRawTX(txRowID: Future<Long>, tx: ZMQRawTX) = doLater {
    connection.prepareStatement(template_insertMempoolTX).use {
        it.setLong(1, txRowID.get())
        it.setLong(2, tx.time)
        it.setLong(3, tx.blockHeight)
        it.setInt(4, tx.txn)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertMintedTX(txRowID: Future<Long>, mintedTX: DB.MintedTX) = doLater {
    connection.prepareStatement(template_insertMintedTX).use {
        it.setLong(1, txRowID.get())
        it.setLong(2, mintedTX.blockHeight)
        it.setInt(3, mintedTX.txn)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertTX(txID: String, txType: String, fee: BigDecimal, isConfirmed: Boolean): Future<Long> {
    val rowID = Future<Long>()

    doLater {
        val txTypeRowID = insertTransactionType(txType)

        connection.prepareStatement(template_insertTX).use {
            it.setString(1, txID)
            it.setInt(2, txTypeRowID)
            it.setBigDecimal(3, fee)
            it.setBoolean(4, isConfirmed)

            it.setBigDecimal(5, fee)
            it.setBoolean(6, isConfirmed)
            rowID.set(upsertReturning(it))
        }
    }
    return rowID
}

private fun DBTX.insertTransactionType(txType: String): Int {
    connection.prepareStatement(template_insertTXType).use {
        it.setString(1, txType)
        return upsertReturning(it).toInt()
    }
}