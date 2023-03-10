package com.trader.defichain

import com.trader.defichain.auction.broadcastAuctions
import com.trader.defichain.config.RPCConfig
import com.trader.defichain.config.ZMQConfig
import com.trader.defichain.config.rpcConfig
import com.trader.defichain.config.zmqConfig
import com.trader.defichain.dex.broadcastDEX
import com.trader.defichain.dex.cachePoolPairs
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.mempool.sendMempoolEvents
import com.trader.defichain.plugins.configureContentNegotiation
import com.trader.defichain.plugins.configureRouting
import com.trader.defichain.plugins.configureWebsockets
import com.trader.defichain.telegram.approveNewNotifications
import com.trader.defichain.telegram.broadcastTelegramMessages
import com.trader.defichain.telegram.loadNotifications
import com.trader.defichain.telegram.notifyTelegramSubscribers
import com.trader.defichain.wallet.broadcastOrderUpdates
import com.trader.defichain.zmq.receiveFullNodeEvents
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import org.zeromq.ZContext
import java.nio.file.Files
import java.nio.file.Paths

lateinit var applicationEngine: ApplicationEngine
lateinit var appServerConfig: AppServerConfig

fun loadAppServerConfig(path: String) {
    appServerConfig = Json { ignoreUnknownKeys = true }.decodeFromStream(Files.newInputStream(Paths.get(path)))

    zmqConfig = appServerConfig.zmq
    rpcConfig = appServerConfig.rpc
}

fun main(vararg args: String) {
    loadAppServerConfig(args[0])
    runBlocking {
        cachePoolPairs()
    }

    if (appServerConfig.production) {
        println("Launching Telegram account authenticator")

        GlobalScope.launch {
            broadcastTelegramMessages(coroutineContext)
        }

        GlobalScope.launch {
            approveNewNotifications(coroutineContext)
        }

        GlobalScope.launch {
            notifyTelegramSubscribers(coroutineContext)
        }
        loadNotifications()
    }

    GlobalScope.launch(Dispatchers.IO) {
        broadcastDEX(coroutineContext)
    }

    GlobalScope.launch(Dispatchers.IO) {
        broadcastAuctions(coroutineContext)
    }

    GlobalScope.launch(Dispatchers.IO) {
        receiveFullNodeEvents(ZContext(), coroutineContext)
    }

    GlobalScope.launch(Dispatchers.IO) {
        broadcastOrderUpdates(coroutineContext)
    }

    if (appServerConfig.production) {
        GlobalScope.launch(Dispatchers.IO) {
            sendMempoolEvents(coroutineContext)
        }
    }

    applicationEngine = embeddedServer(Netty, port = appServerConfig.port, host = appServerConfig.host) {
        configureWebsockets()
        configureContentNegotiation()
        configureRouting()
    }

    applicationEngine.start(wait = true)
}

@kotlinx.serialization.Serializable
data class TelegramBotConfig(
    val url: String,
    val secret: String,
) {
    fun createURL(uri: String): String {
        check(!uri.startsWith("/"))
        return "$url/$secret/$uri"
    }
}

@kotlinx.serialization.Serializable
data class AppServerConfig(
    val production: Boolean,
    val local: Boolean = true,
    val telegramBot: TelegramBotConfig,
    val webappRoot: String,
    val host: String,
    val port: Int,
    val rpc: RPCConfig,
    val zmq: ZMQConfig,
    val accountsRoot: String,
) {

    init {
        if (production) {
            val webappRootPath = Paths.get(webappRoot)
            check(Files.exists(webappRootPath) && Files.isDirectory(webappRootPath))

            val buildDirPath = webappRootPath.resolve("build")
            check(Files.exists(buildDirPath) && Files.isDirectory(buildDirPath))

            val indexHTML = webappRootPath.resolve("index.html")
            check(Files.exists(indexHTML) && Files.isRegularFile(indexHTML))

            val accountRootPath = Paths.get(accountsRoot)
            check(Files.exists(accountRootPath) && Files.isDirectory(accountRootPath))
        }
    }
}

class App {

    companion object {
        var zmqSubscriberConnected = false
    }
}
