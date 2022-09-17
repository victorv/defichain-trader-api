package com.trader.defichain.db

private val template_insertAddress = """
    INSERT INTO address (dc_address) VALUES (?)
    ON CONFLICT (dc_address) DO UPDATE SET dc_address = address.dc_address
    RETURNING row_id;
    """.trimIndent()

fun DBTX.insertAddress(
    address: String,
): Long {
    dbUpdater.prepareStatement(template_insertAddress).use {
        it.setString(1, address)
        return upsertReturning(it)
    }
}