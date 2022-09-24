package com.trader.defichain.db

import com.trader.defichain.rpc.CustomTX
import com.trader.defichain.util.Future

private val template_insertLoanOrCollateral = """
INSERT INTO loan (tx_row_id, vault, owner) 
VALUES (?, ?, ?)
ON CONFLICT(tx_row_id) DO UPDATE SET
vault = ?,
owner = ?
""".trimIndent()

fun DBTX.storeLoanOrCollateral(
    txRowID: Future<Long>,
    tableName: String,
    loanOrCollateral: CustomTX.CollateralOrLoan,
) = doLater {
    insertTokenAmounts(txRowID.get(), loanOrCollateral.amounts)

    val vault = insertVault(loanOrCollateral.vaultID)
    val owner = insertAddress(loanOrCollateral.owner())

    val template = template_insertLoanOrCollateral.replaceFirst("loan", tableName)
    connection.prepareStatement(template).use {
        it.setLong(1, txRowID.get())
        it.setLong(2, vault)
        it.setLong(3, owner)

        it.setLong(4, vault)
        it.setLong(5, owner)

        check(it.executeUpdate() <= 1)
    }
}