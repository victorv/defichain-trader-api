package com.trader.defichain.dex

import com.trader.defichain.http.Connection
import com.trader.defichain.http.Message
import com.trader.defichain.http.connections
import com.trader.defichain.zmq.newZQMBlockChannel
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlin.coroutines.CoroutineContext

private val blockChannel = newZQMBlockChannel()

suspend fun broadcastDEX(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        try {
            broadcast()
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    }
}

private suspend fun broadcast() {
    blockChannel.receive()

    for (connection in connections) {

        try {
            for (poolSwap in connection.poolSwaps) {
                sendSwapResult("swap-result", poolSwap, connection)
            }
        } catch (e: Throwable) {
            e.printStackTrace()
            connection.close()
        }
    }
}

private suspend fun sendSwapResult(
    id: String,
    swap: PoolSwap,
    connection: Connection
) {
    val swapResult = testPoolSwap(swap)
    val message = Message(
        id = id,
        data = Json.encodeToJsonElement(swapResult),
    )
    connection.send(Json.encodeToString(message))
}