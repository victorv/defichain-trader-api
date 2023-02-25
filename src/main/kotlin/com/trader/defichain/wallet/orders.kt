package com.trader.defichain.wallet

import com.trader.defichain.dex.LimitOrder
import com.trader.defichain.dex.SwapResult
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.http.Message
import com.trader.defichain.http.connections
import com.trader.defichain.rpc.RPC
import com.trader.defichain.zmq.newZQMBlockChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.coroutines.CoroutineContext

private val orders = CopyOnWriteArrayList<LimitOrder>()
private val blockChannel = newZQMBlockChannel()
private var textMessage = "[]"

suspend fun addLimitOrder(order: LimitOrder): Boolean {
    if (orders.size >= 25) return false

    val swap = RPC.decodeCustomTX(order.signedTX)?.asPoolSwap() ?: return false
    check(swap.fromAddress == swap.toAddress)
    val result = testPoolSwap(order.asPoolSwap())
    check(result.estimate > 0.0)
    orders.add(order)
    createTextMessage()
    return true
}

fun getOrders() = textMessage

suspend fun broadcastOrderUpdates(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        try {
            blockChannel.receive()

            createTextMessage()

            for (connection in connections) {
                if(connection.hasAddress) {
                    connection.send(textMessage)
                }
            }
        } catch (e: Throwable) {

        } finally {
            delay(5000)
        }


    }
}

private fun createTextMessage() {
    val results = ArrayList<SwapResult>(orders.size)
    for (order in orders) {
        results.add(testPoolSwap(order.asPoolSwap()))
    }

    val data = Json.encodeToJsonElement(results)
    val message = Message("orders", data)
    textMessage = Json.encodeToString(message)
}

