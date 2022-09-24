package com.trader.defichain.db

// TODO process transactions that create vaults to determine the vault owner address even if the vault has been deleted
private val template_insertVault = """
    INSERT INTO vault (dc_vault_id, owner) VALUES (?, null)
    ON CONFLICT (dc_vault_id) DO UPDATE SET dc_vault_id = vault.dc_vault_id
    RETURNING row_id;
    """.trimIndent()

fun DBTX.insertVault(
    vaultID: String,
): Long {
    connection.prepareStatement(template_insertVault).use {
        it.setString(1, vaultID)
        return upsertReturning(it)
    }
}