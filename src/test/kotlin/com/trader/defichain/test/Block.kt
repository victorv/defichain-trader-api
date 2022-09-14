package com.trader.defichain.test

import com.trader.defichain.rpc.RPCMethod
import com.trader.defichain.rpc.RPCRequest
import io.ktor.client.engine.mock.*
import io.ktor.client.request.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import kotlin.io.path.inputStream

private val blocks = loadCapturedBlocks()
private var block = blocks[0]

fun submitBlock(index: Int) {
    block = blocks[index]
}

fun getBlock() = block

suspend fun getResponseBody(request: HttpRequestData): String? {
    val requestBodyAsString = request.body.toByteArray().decodeToString()
    val requestBody = Json.decodeFromString<RPCRequest>(requestBodyAsString)

    if (requestBody.method == RPCMethod.GET_BLOCK_COUNT.id) {
        return block.heightJson
    }
    if (requestBody.method == RPCMethod.LIST_TOKENS.id) {
        return block.tokensJson
    }
    if (requestBody.method == RPCMethod.LIST_POOL_PAIRS.id) {
        return block.poolPairsJson
    }
    if (requestBody.method == RPCMethod.LIST_PRICES.id) {
        return block.pricesJson
    }
    return null
}

private fun loadCapturedBlocks(): List<CapturedBlock> {
    val testResourcesDir = getTestResourcesDirectory()
    val blocksPath = testResourcesDir.resolve("blocks.json")
    if (Files.exists(blocksPath)) {
        blocksPath.inputStream(StandardOpenOption.READ).use {
            return Json.decodeFromStream(it)
        }
    }
    return listOf()
}