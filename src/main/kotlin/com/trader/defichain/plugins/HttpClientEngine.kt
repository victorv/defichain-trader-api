package com.trader.defichain.plugins

import io.ktor.client.engine.*
import io.ktor.client.engine.java.*

private var engine: HttpClientEngine = Java.create()

fun overrideHttpClientEngine(httpClientEngine: HttpClientEngine) {
    engine = httpClientEngine
}

fun getHttpClientEngine() = engine
