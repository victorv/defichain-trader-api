package com.trader.defichain.indexer

import com.trader.defichain.dex.PoolPair
import com.trader.defichain.rpc.*
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlin.coroutines.CoroutineContext

suspend fun indexPoolPairs(coroutineContext: CoroutineContext) {
    var blockCount = RPC.getValue<Int>(
        RPCRequest(
            instance = RPCInstance.STANDBY,
            method = RPCMethod.GET_BLOCK_COUNT.id,
            jsonrpc = "1.0",
            params = JsonArray(emptyList()),
        )
    )

    while (coroutineContext.isActive) {

        val newBlockCount = blockCount + 1

        val hash = RPC.getValue<String>(
            RPCRequest(
                instance = RPCInstance.LIVE,
                method = RPCMethod.GET_BLOCK_HASH.id,
                jsonrpc = "1.0",
                params = JsonArray(listOf(JsonPrimitive(newBlockCount)))
            )
        )

        val rawBlock = RPC.getValue<String>(
            RPCRequest(
                instance = RPCInstance.LIVE,
                method = RPCMethod.GET_BLOCK.id,
                jsonrpc = "1.0",
                params = JsonArray(listOf(JsonPrimitive(hash), JsonPrimitive(false)))
            )
        )

        val submissionResult = RPC.tryGet<String>(
            RPCRequest(
                instance = RPCInstance.STANDBY,
                method = RPCMethod.SUBMIT_BLOCK.id,
                jsonrpc = "1.0",
                params = JsonArray(listOf(JsonPrimitive(rawBlock))),
                noResponse = true,
            )
        )
        println(submissionResult)

        blockCount = RPC.getValue(
            RPCRequest(
                instance = RPCInstance.STANDBY,
                method = RPCMethod.GET_BLOCK_COUNT.id,
                jsonrpc = "1.0",
                params = JsonArray(emptyList()),
            )
        )

        if (blockCount == newBlockCount) {
            val poolPairs = RPC.getValue<Map<String, PoolPair>>(
                RPCRequest(
                    instance = RPCInstance.STANDBY,
                    method = RPCMethod.LIST_POOL_PAIRS.id,
                    jsonrpc = "1.0",
                    params = JsonArray(listOf(limit1000)),
                )
            ).entries.associate { it.key.toInt() to it.value }

            println("Success: $newBlockCount!")
            println(poolPairs)
        } else {
            println("failed")
        }
    }
}