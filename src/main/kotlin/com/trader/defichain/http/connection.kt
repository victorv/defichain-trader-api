package com.trader.defichain.http

import com.trader.defichain.db.search.SearchFilter
import com.trader.defichain.dex.PoolSwap
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

val connections: CopyOnWriteArrayList<Connection> = CopyOnWriteArrayList()

class Connection {

    var description: String? = null
    var filter: SearchFilter? = null
    val uuid = UUID.randomUUID().toString()
    private val writableChannel = Channel<String>(10, BufferOverflow.DROP_OLDEST)
    val poolSwaps = CopyOnWriteArrayList<PoolSwap>()
    val channel = writableChannel as ReceiveChannel<String>

    suspend fun send(message: String) {
        writableChannel.send(message)
    }

    fun close() {
        writableChannel.close()
    }

    suspend fun sendUUID() {
        send(Json.encodeToString(Message(id = "uuid", data = JsonPrimitive(uuid))))
    }
}