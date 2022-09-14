package com.trader.defichain.rpc

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

@kotlinx.serialization.Serializable
data class CustomTX(
    val type: String,
    val valid: Boolean,
    val results: JsonObject
)

@kotlinx.serialization.Serializable
data class Token (
    val symbol: String,
    val symbolKey: String,
    val name: String,
    val decimal: Int,
    val limit: Int,
    val mintable: Boolean,
    val tradeable: Boolean,
    val isDAT: Boolean,
    val isLPS: Boolean,
    val finalized: Boolean,
    val isLoanToken: Boolean,
    val minted: Double,
    val creationTx: String,
    val creationHeight: Int,
    val destructionTx: String,
    val destructionHeight: Int,
    val collateralAddress: String,
    var oraclePrice: Double? = null // assigned at later stage
)

@kotlinx.serialization.Serializable
data class OraclePrice(
    val token: String,
    val currency: String? = null,
    val price: Double? = null,
    val ok: JsonPrimitive,
)

@kotlinx.serialization.Serializable
data class MempoolEntry(
    val fee: Double,
    val height: Int,
    val time: Long,
)

@kotlinx.serialization.Serializable
data class Block(
    val height: Int,
    val tx: List<TX>,
)

enum class RPCMethod(val id: String) {
    GET_RAW_MEMPOOL("getrawmempool"),
    DECODE_RAW_TRANSACTION("decoderawtransaction"),
    GET_CUSTOM_TX("getcustomtx"),
    TEST_POOL_SWAP("testpoolswap"),
    GET_BLOCK_COUNT("getblockcount"),
    LIST_POOL_PAIRS("listpoolpairs"),
    LIST_TOKENS("listtokens"),
    LIST_PRICES("listprices"),
    GET_BEST_BLOCK_HASH("getbestblockhash"),
    GET_BLOCK("getblock"),
    LIST_ACCOUNT_HISTORY("listaccounthistory"),
    DECODE_CUSTOM_TX("decodecustomtx"),
    GET_MEMPOOL_ENTRY("getmempoolentry"),
    GET_RAW_TRANSACTION("getrawtransaction");
}