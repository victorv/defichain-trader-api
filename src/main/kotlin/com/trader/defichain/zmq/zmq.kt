package com.trader.defichain.zmq

import com.trader.defichain.App
import com.trader.defichain.config.zmqConfig
import com.trader.defichain.dex.PoolPair
import com.trader.defichain.dex.cachePoolPairs
import com.trader.defichain.util.toHex2
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMsg
import java.nio.charset.StandardCharsets
import kotlin.coroutines.CoroutineContext

private var poolPairs: Map<Int, PoolPair>? = null
private val blockChannels = ArrayList<Channel<Boolean>>()
private val eventChannels = ArrayList<Channel<ZMQEvent>>()

fun newZQMBlockChannel(): Channel<Boolean> {
    val channel = Channel<Boolean>(1, BufferOverflow.DROP_OLDEST)
    blockChannels.add(channel)
    return channel
}

fun newZMQEventChannel(): Channel<ZMQEvent> {
    val channel = Channel<ZMQEvent>(10000, BufferOverflow.DROP_OLDEST)
    eventChannels.add(channel)
    return channel
}

suspend fun receiveFullNodeEvents(zmqContext: ZContext, coroutineContext: CoroutineContext) {
    zmqContext.use { context ->
        while (coroutineContext.isActive) {
            try {
                context.createSocket(SocketType.SUB).use { subscriber ->
                    check(subscriber.connect("tcp://${zmqConfig.host}:${zmqConfig.port}"))
                    App.zmqSubscriberConnected = true
                    subscriber.subscribe(ZMQEventType.HASH_BLOCK.code.toByteArray(ZMQ.CHARSET))
                    subscriber.subscribe(ZMQEventType.RAW_TX.code.toByteArray(ZMQ.CHARSET))

                    while (coroutineContext.isActive) {

                        val frames = ZMsg.recvMsg(subscriber).toList()
                        when (val topic = frames[0].getString(StandardCharsets.UTF_8)) {
                            ZMQEventType.HASH_BLOCK.code -> {
                                val blockHash = frames[1].data.toHex2()

                                poolPairs = cachePoolPairs()

                                for (channel in eventChannels) {
                                    channel.send(ZMQEvent(ZMQEventType.HASH_BLOCK, blockHash, poolPairs))
                                }
                                for (channel in blockChannels) {
                                    channel.send(true)
                                }
                            }
                            ZMQEventType.RAW_TX.code -> {
                                for (channel in eventChannels) {
                                    val rawTX = frames[1].data.toHex2()
                                    channel.send(ZMQEvent(ZMQEventType.RAW_TX, rawTX, poolPairs))
                                }
                            }
                            else -> throw IllegalStateException("unsupported topic: $topic")
                        }
                    }
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
    }
}

enum class ZMQEventType(val code: String) {
    HASH_BLOCK("hashblock"),
    RAW_TX("rawtx")
}

data class ZMQEvent(
    val type: ZMQEventType,
    val payload: String,
    val poolPairs: Map<Int, PoolPair>?
)