package com.trader.defichain.http

import com.trader.defichain.dex.PoolSwap
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import java.util.concurrent.CopyOnWriteArrayList

val connections: CopyOnWriteArrayList<Connection> = CopyOnWriteArrayList()

class Connection {

    private var uuid: String? = null
    private val writableChannel = Channel<String>(10, BufferOverflow.DROP_OLDEST)
    val poolSwaps = CopyOnWriteArrayList<PoolSwap>()
    val channel = writableChannel as ReceiveChannel<String>
    var graph: SetGraph? = null

    suspend fun send(message: String) {
        if (uuid != null) {
            writableChannel.send(message)
        }
    }

    fun close() {
        writableChannel.close()
    }

    fun setUUID(uuid: String) {
        this.uuid = uuid
    }
}