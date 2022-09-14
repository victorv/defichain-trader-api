package com.trader.defichain.test

import com.trader.defichain.plugins.overrideHttpClientEngine
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import io.ktor.utils.io.*

fun installHttpClientMockEngine() {
    val mockEngine = MockEngine { request ->
        val responseBody = getResponseBody(request)
        check(responseBody != null) { "Response not found" }

        respond(
            content = ByteReadChannel(responseBody),
            status = HttpStatusCode.OK,
            headers = headersOf(HttpHeaders.ContentType, "application/json")
        )
    }
    overrideHttpClientEngine(mockEngine)
}

