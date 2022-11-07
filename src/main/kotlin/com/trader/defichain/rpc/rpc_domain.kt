package com.trader.defichain.rpc

import kotlinx.serialization.json.JsonNames
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigDecimal

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
    val height: Int,
    val hash: String,
    val tx: List<TX>,
    @JsonNames("mediantime")
    val medianTime: Long,
    @JsonNames("previousblockhash")
    val previousBlockHash: String? = null,
    val minter: String,
    @JsonNames("masternode")
    val masterNode: String,
) {
    init {
        tx.forEachIndexed { txn, tx -> tx.txn = txn }
    }
}

data class MasterNodeTX(val tx: TX, val fee: BigDecimal) {
    val type = "CreateMasterNode"
    val size = tx.size
}

enum class RPCMethod(val id: String) {
    DECODE_RAW_TRANSACTION("decoderawtransaction"),
    TEST_POOL_SWAP("testpoolswap"),
    GET_BLOCK_COUNT("getblockcount"),
    LIST_POOL_PAIRS("listpoolpairs"),
    LIST_TOKENS("listtokens"),
    LIST_PRICES("listprices"),
    GET_BLOCK("getblock"),
    GET_ACCOUNT_HISTORY("getaccounthistory"),
    DECODE_CUSTOM_TX("decodecustomtx"),
    GET_RAW_TRANSACTION("getrawtransaction"),
    GET_BLOCK_HASH("getblockhash"),
    GET_VAULT("getvault");
}