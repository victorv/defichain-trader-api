package com.trader.defichain.dex

import com.trader.defichain.zmq.newZQMBlockChannel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.coroutines.CoroutineContext

private val blockHashChannel: ReceiveChannel<Boolean> = newZQMBlockChannel()
private val subscribers = CopyOnWriteArrayList<DEXSubscriber>()

fun addSubscriber(subscriber: DEXSubscriber) {
    subscribers.add(subscriber)
    subscriber.isActive = true
}

fun removeSubscriber(subscriber: DEXSubscriber) {
    subscribers.remove(subscriber)
    subscriber.isActive = false
}

suspend fun pingDEXSubscribers(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        for (subscriber in subscribers) {
            try {
                subscriber.pendingMessages.send("event: ping\n\n")
            } catch (e: Throwable) {
                removeSubscriber(subscriber)
            }
        }
        delay(10000)
    }
}

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
    blockHashChannel.receive()

    for (subscriber in subscribers) {
        val swapResults = ArrayList<SwapResult?>()
        try {
            for (poolSwap in subscriber.poolSwaps) {
                val testResult = testPoolSwap(poolSwap)
                swapResults.add(testResult)
            }

            val data = Json.encodeToString(swapResults)
            subscriber.pendingMessages.send("event: poolswap\ndata: $data\n\n")
        } catch (e: Throwable) {
            removeSubscriber(subscriber)
            subscriber.pendingMessages.close(e)
        }
    }
}