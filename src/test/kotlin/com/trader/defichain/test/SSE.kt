package com.trader.defichain.test

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeoutException

private const val joinTimeout = 10000
private const val pingEvent = "event: ping"
private val allSSEJobs = CopyOnWriteArrayList<SSEClient>()

fun joinSSEJobs() {
    val startTime = System.currentTimeMillis()
    while (allSSEJobs.isNotEmpty()) {
        Thread.sleep(50)
        if (System.currentTimeMillis() - startTime > joinTimeout) {
            throw IllegalStateException("Failed to stop all SSE-client-jobs")
        }
    }
}

class SSEClientEngine {

    private val sseJobs = CopyOnWriteArrayList<SSEClient>()

    fun cancelSSEJobs() {
        sseJobs.forEach { it.setDisconnected() }
    }

    fun receiveServerSentEvents(path: String): SSEClient {
        val reader = connectTo(path)
        val client = SSEClient()

        Thread {
            try {
                sseJobs.add(client)
                allSSEJobs.add(client)

                fetchEvents(reader, client)
            } finally {
                client.setDisconnected()
                allSSEJobs.remove(client)
            }
        }.start()

        return client
    }
}

private fun fetchEvents(reader: InputStream, client: SSEClient) {
    reader.bufferedReader(StandardCharsets.UTF_8).use { reader ->
        do {
            val line = reader.readLine() ?: break
            client.addLine(line)
        } while (!client.isDisconnected())
    }
}

private fun connectTo(path: String): InputStream {
    val url = URL("${serverURL}/$path")
    val connection = url.openConnection() as HttpURLConnection
    connection.setRequestProperty("accept", "application/json")

    if (connection.responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
        throw IllegalStateException(connection.errorStream.readAllBytes().decodeToString())
    }
    return connection.inputStream
}


class SSEClient {

    private companion object {
        const val readTimeout = 5000
        const val linesPerEvent = 3
        const val retryPrefix = "retry: "
        const val eventNamePrefix = "event: "
        const val eventDataPrefix = "data: "
        val eventNameMatcher = Regex("^[a-z]+$")
    }

    private var disconnected = false
    private var eventsRead = 0
    private val lines = LinkedList<String>()
    private var prevLine: String? = null

    fun addLine(line: String) {
        if (line != pingEvent && prevLine != pingEvent) {
            lines.add(line)
        }
        prevLine = line
    }

    fun isDisconnected() = disconnected

    fun setDisconnected() {
        disconnected = true
    }

    fun hasLines() = lines.isNotEmpty()

    fun getLines(): List<String> = lines

    inline fun <reified T> nextEvent(): SSE<T> {
        val sse = getNextEvent()
        return SSE(
            name = sse.name,
            data = Json.decodeFromString(sse.data),
            retry = sse.retry
        )
    }

    fun getNextEvent(): SSE<String> {
        check(!disconnected) { "You are no longer connected to the server" }

        val requiredLines = if (eventsRead == 0) linesPerEvent + 1 else linesPerEvent

        val startTime = System.currentTimeMillis()
        while (lines.size < requiredLines) {
            Thread.sleep(50)
            val timePassed = System.currentTimeMillis() - startTime
            if (lines.size < requiredLines && timePassed > readTimeout) {
                throw TimeoutException("Queue does not contain any events. Gave up after waiting ${readTimeout}ms.")
            }
        }

        var retry: Int? = null
        if (eventsRead == 0) {
            val retryLine = lines.poll()
            check(retryLine.length > retryPrefix.length) {
                "Line is too short to contain retry: `$retryLine`"
            }
            check(retryLine.startsWith(retryPrefix)) {
                "Expected line to start with `$retryPrefix`, got: `$retryLine`"
            }

            retry = retryLine.removePrefix(retryPrefix).toInt()
            check(retry == 30000)
        }

        val eventNameLine = lines.poll()
        check(eventNameLine.length > eventNamePrefix.length) {
            "Line is too short to contain an event name: `$eventNameLine`"
        }
        check(eventNameLine.startsWith(eventNamePrefix)) {
            "Expected line to start with `$eventNamePrefix`, got: `$eventNameLine`"
        }

        val eventName = eventNameLine.removePrefix(eventNamePrefix)
        check(eventNameMatcher.matches(eventName)) {
            "Expected event name to match `${eventNameMatcher.pattern}`, got: `$eventName`"
        }

        val eventDataLine = lines.poll()
        check(eventDataLine.length > eventDataPrefix.length) {
            "Line for event `$eventName` is too short to contain event data: `$eventDataLine`"
        }
        check(eventDataLine.startsWith(eventDataPrefix)) {
            "Expected line for event `$eventName` to start with `$eventDataPrefix`, got: `$eventDataLine`"
        }

        val end = lines.poll()
        check(end.isEmpty()) { "Event `$eventName` should be proceeded by a \\n character" }

        val eventData = eventDataLine.removePrefix(eventDataPrefix)

        eventsRead++

        return SSE(
            name = eventName,
            data = eventData,
            retry = retry
        )
    }
}

data class SSE<T>(val name: String, val data: T, val retry: Int?)