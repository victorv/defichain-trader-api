package com.trader.defichain.indexer

import com.trader.defichain.config.RPCConfig
import com.trader.defichain.config.ZMQConfig
import com.trader.defichain.config.rpcConfig
import com.trader.defichain.config.zmqConfig
import com.trader.defichain.db.DB
import com.trader.defichain.zmq.receiveFullNodeEvents
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import org.zeromq.ZContext
import java.nio.file.Files
import java.nio.file.Paths

val initialDatabaseUpdater = DB.createUpdater()
const val BLOCK_TIME = 30000L
private fun loadIndexerConfig(path: String) {
    val config = Json.decodeFromStream<IndexerConfig>(Files.newInputStream(Paths.get(path)))

    zmqConfig = config.zmq
    rpcConfig = config.rpc
}

fun main(vararg args: String) {
    loadIndexerConfig(args[0])
    runBlocking {
        initialDatabaseUpdater.doTransaction {
            indexTokens(it)
        }
    }

    val scopes = ArrayList<Job>()
    scopes += GlobalScope.launch {
        receiveFullNodeEvents(ZContext(), coroutineContext)
    }

    scopes += GlobalScope.launch {
        announceZMQBatches(coroutineContext)
    }

    scopes += GlobalScope.launch {
        indexZMQBatches(coroutineContext)
    }

    scopes += GlobalScope.launch {
        processZMQEvents(coroutineContext)
    }

    runBlocking {
        scopes.joinAll()
    }
}

@kotlinx.serialization.Serializable
data class IndexerConfig(
    val rpc: RPCConfig,
    val zmq: ZMQConfig,
)