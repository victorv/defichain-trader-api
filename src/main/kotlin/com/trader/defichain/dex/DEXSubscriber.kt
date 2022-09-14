package com.trader.defichain.dex

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext

data class DEXSubscriber(
    val coroutineContext: CoroutineContext,
    val poolSwaps: List<PoolSwap>,
) {
    val pendingMessages = Channel<String>(10, BufferOverflow.DROP_OLDEST)
    var written = 0
    var isActive = false
}