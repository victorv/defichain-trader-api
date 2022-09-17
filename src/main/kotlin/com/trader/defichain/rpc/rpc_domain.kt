package com.trader.defichain.rpc

import kotlinx.serialization.json.JsonNames
import kotlinx.serialization.json.JsonPrimitive

@kotlinx.serialization.Serializable
data class Token(
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
data class Block(
    val height: Long,
    val hash: String,
    val tx: List<TX>,
    val time: Long,
    @JsonNames("previousblockhash")
    val previousBlockHash: String? = null,
) {
    init {
        tx.forEachIndexed { txn, tx -> tx.txn = txn }
    }
}

enum class RPCMethod(val id: String) {
    DECODE_RAW_TRANSACTION("decoderawtransaction"),
    TEST_POOL_SWAP("testpoolswap"),
    GET_BLOCK_COUNT("getblockcount"),
    LIST_POOL_PAIRS("listpoolpairs"),
    LIST_TOKENS("listtokens"),
    LIST_PRICES("listprices"),
    GET_BEST_BLOCK_HASH("getbestblockhash"),
    GET_BLOCK("getblock"),
    GET_ACCOUNT_HISTORY("getaccounthistory"),
    DECODE_CUSTOM_TX("decodecustomtx"),
    GET_RAW_TRANSACTION("getrawtransaction");
}