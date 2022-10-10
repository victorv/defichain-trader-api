package com.trader.defichain.test

import com.trader.defichain.dex.AbstractPoolSwap
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.http.Message
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.http.*
import io.ktor.websocket.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.encodeToJsonElement
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList

private const val joinTimeout = 10000
private const val pingEvent = "event: ping"
private val allSSEJobs = CopyOnWriteArrayList<WebsocketClient>()

fun joinSSEJobs() {
    val startTime = System.currentTimeMillis()
    while (allSSEJobs.isNotEmpty()) {
        Thread.sleep(50)
        if (System.currentTimeMillis() - startTime > joinTimeout) {
            throw IllegalStateException("Failed to stop all SSE-client-jobs")
        }
    }
}

class WebsocketClient(private val client: HttpClient) {

    private val messages = CopyOnWriteArrayList<Message>()
    fun addMessage(data: ByteArray) {
        messages.add(Json.decodeFromString(data.decodeToString()))
    }

    fun receive(): Message {
        var i = 0
        while (messages.isEmpty() && i < 60) {
            Thread.sleep(50)
            i++
        }
        return messages.removeFirst()
    }

    fun close() {
        client.close()
    }
}

class SSEClientEngine {

    private val clients = CopyOnWriteArrayList<WebsocketClient>()

    fun closeClients() {
        clients.forEach { it.close() }
    }

    fun receiveMessages(swaps: List<AbstractPoolSwap>): WebsocketClient {
        val websocketClient = HttpClient(CIO) {
            install(WebSockets)
        }


//        val reader = connectTo(path)

        var client = WebsocketClient(websocketClient)
        Thread {

            try {
                runBlocking {
                    websocketClient.webSocket(HttpMethod.Get, serverURL, serverPort, "/stream") {
                        clients.add(client)
                        allSSEJobs.add(client)

                        send(
                            Json.encodeToString(
                                Message(
                                    id = "uuid",
                                    data = JsonPrimitive(UUID.randomUUID().toString()),
                                )
                            )
                        )

                        for (swap in swaps) {
                            val message = Message(
                                id = "add-swap",
                                data = Json.encodeToJsonElement(
                                    PoolSwap(
                                        amountFrom = swap.amountFrom,
                                        tokenFrom = swap.tokenFrom,
                                        tokenTo = swap.tokenTo,
                                        desiredResult = 1.0,
                                    )
                                )
                            )
                            send(Json.encodeToString(message).toByteArray())
                        }

                        var responsesReceived = 0
                        for (message in incoming) {
                            responsesReceived++
                            if (responsesReceived == swaps.size) {
                                publishZMQMessage()
                                publishZMQMessage()
                            }

                            client?.addMessage(message.data)
                        }
                    }
                }
            } finally {
                client?.close()
                allSSEJobs.remove(client)
            }
        }.start()

        return client
    }
}