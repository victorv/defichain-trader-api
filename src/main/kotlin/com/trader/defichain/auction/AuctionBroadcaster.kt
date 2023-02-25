package com.trader.defichain.auction

import com.trader.defichain.http.Message
import com.trader.defichain.http.connections
import com.trader.defichain.zmq.newZQMBlockChannel
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlin.coroutines.CoroutineContext

private val blockChannel = newZQMBlockChannel()
private var auctions: List<Auction> = emptyList()

suspend fun broadcastAuctions(coroutineContext: CoroutineContext) {
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

    val newAuctions = listAuctions()
    if (auctions == newAuctions) {
        return
    }
    auctions = newAuctions

    val message = Json.encodeToString(Message(
        id = "auctions",
        data = Json.encodeToJsonElement(auctions),
    ))

    for (connection in connections) {
        try {
            connection.send(message)
        } catch (e: Throwable) {
            e.printStackTrace()
            connection.close()
        }
    }
}