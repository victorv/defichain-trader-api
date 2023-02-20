package com.trader.defichain.db

import com.trader.defichain.indexer.ZMQRawTX
import com.trader.defichain.rpc.Block
import com.trader.defichain.util.Future
import java.math.BigDecimal

private val fi = BigDecimal(100000000)

private const val template_insertBlock = """
INSERT INTO block (height, time, hash, finalized, dirty, master_node, minter) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (height) DO UPDATE SET time = ?, finalized = ?, dirty = ?, master_node = ?, minter = ?
"""

private const val template_insertMempoolTX = """
INSERT INTO mempool (tx_row_id, time, block_height, txn) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_row_id) DO NOTHING
"""

private const val template_insertMintedTX = """
INSERT INTO minted_tx (tx_row_id, block_height, txn) VALUES (?, ?, ?)
ON CONFLICT DO NOTHING
"""

private const val template_insertTXType = """
INSERT INTO tx_type (dc_tx_type) VALUES (?)
ON CONFLICT (dc_tx_type) DO UPDATE SET dc_tx_type = tx_type.dc_tx_type
RETURNING row_id;
"""

private const val template_insertTX = """
INSERT INTO tx (dc_tx_id, tx_type_row_id, fee, size, fee_rate, confirmed, valid) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (dc_tx_id) DO UPDATE SET 
fee = ?,
size = ?,
fee_rate = ?,
confirmed = ?,
valid = ?
RETURNING row_id;
"""

fun DBTX.insertBlock(block: Block, masterNode: Future<Long>, finalized: Boolean) = doLater {
    val minter = insertAddress(block.minter ?: "placeholderaddress")
    val isBlockDirty = !finalized || isDirty

    connection.prepareStatement(template_insertBlock).use {
        it.setInt(1, block.height)
        it.setLong(2, block.medianTime)
        it.setString(3, block.hash)
        it.setBoolean(4, finalized)
        it.setBoolean(5, isBlockDirty)
        it.setLong(6, masterNode.get())
        it.setLong(7, minter)

        it.setLong(8, block.medianTime)
        it.setBoolean(9, finalized)
        it.setBoolean(10, isBlockDirty)
        it.setLong(11, masterNode.get())
        it.setLong(12, minter)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertRawTX(txRowID: Future<Long>, tx: ZMQRawTX) = doLater {
    connection.prepareStatement(template_insertMempoolTX).use {
        it.setLong(1, txRowID.get())
        it.setLong(2, tx.time)
        it.setInt(3, tx.blockHeight)
        it.setInt(4, tx.txn)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertMintedTX(txRowID: Future<Long>, mintedTX: DB.MintedTX) = doLater {
    connection.prepareStatement(template_insertMintedTX).use {
        it.setLong(1, txRowID.get())
        it.setInt(2, mintedTX.blockHeight)
        it.setInt(3, mintedTX.txn)
        insertOrDoNothing(it)
    }
}

fun DBTX.insertTX(txID: String, txType: String, fee: BigDecimal, vsize: Int, isConfirmed: Boolean, valid: Boolean): Future<Long> {
    val rowID = Future<Long>()
    val feeRate = (fee * fi / BigDecimal(vsize))

    doLater {
        val txTypeRowID = insertTransactionType(txType)

        connection.prepareStatement(template_insertTX).use {
            it.setString(1, txID)
            it.setInt(2, txTypeRowID)
            it.setBigDecimal(3, fee)
            it.setInt(4, vsize)
            it.setBigDecimal(5, feeRate)
            it.setBoolean(6, isConfirmed)
            it.setBoolean(7, valid)

            it.setBigDecimal(8, fee)
            it.setInt(9, vsize)
            it.setBigDecimal(10, feeRate)
            it.setBoolean(11, isConfirmed)
            it.setBoolean(12, valid)
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