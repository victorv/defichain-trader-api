package com.trader.defichain.db

import com.trader.defichain.rpc.CustomTX
import com.trader.defichain.util.Future

private val template_insertCollateral = """
    INSERT INTO collateral (tx_row_id, amount, token, vault, owner) 
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(tx_row_id) DO UPDATE SET
    amount = ?,
    token = ?,
    vault = ?,
    owner = ?
    """.trimIndent()

fun DBTX.depositToOrWithdrawFromVault(
    txRowID: Future<Long>,
    collateral: CustomTX.Collateral,
) = doLater {
    val (amount, tokenID) = collateral.amount()

    insertTokens(tokenID)

    val vault = insertVault(collateral.vaultID)
    val owner = insertAddress(collateral.owner())

    connection.prepareStatement(template_insertCollateral).use {
        it.setLong(1, txRowID.get())
        it.setDouble(2, amount)
        it.setInt(3, tokenID)
        it.setLong(4, vault)
        it.setLong(5, owner)

        it.setDouble(6, amount)
        it.setInt(7, tokenID)
        it.setLong(8, vault)
        it.setLong(9, owner)

        check(it.executeUpdate() <= 1)
    }
}