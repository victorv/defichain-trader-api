package com.trader.defichain.test

import com.trader.defichain.applicationEngine
import com.trader.defichain.dex.AbstractPoolSwap
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import java.net.ConnectException
import java.net.HttpURLConnection
import java.net.URL
import kotlin.io.path.absolutePathString

const val serverURL = "127.0.0.1"
const val serverPort = 5555

abstract class UnitTest {

    private lateinit var sseClientEngine: SSEClientEngine

    @BeforeEach
    fun beforeTest() {
        submitBlock(0)
        sseClientEngine = SSEClientEngine()
    }

    @AfterEach
    fun afterTest() {
        sseClientEngine.closeClients()
    }

    fun receiveMessages(swaps: List<AbstractPoolSwap>): WebsocketClient {
        return sseClientEngine.receiveMessages(swaps)
    }

    companion object {
        private lateinit var appThread: Thread

        @BeforeAll
        @JvmStatic
        fun runApp() {
            submitBlock(0)
            installHttpClientMockEngine()

            startZMQPublisher()

            val configPath = getTestResourcesDirectory().resolve("config.json").absolutePathString()
            appThread = Thread {
                com.trader.defichain.main(configPath)
            }
            appThread.start()

            val startTime = System.currentTimeMillis()
            val statusCheck = URL("http://${serverURL}/status")
            var responseCode = 0
            do {
                try {
                    responseCode = (statusCheck.openConnection() as HttpURLConnection).responseCode
                } catch (e: ConnectException) {
                    // retry after a delay
                }
                Thread.sleep(250)
            } while (responseCode != 200 && System.currentTimeMillis() - startTime < 10000)
        }

        @AfterAll
        @JvmStatic
        fun stopApp() {
            // TODO stop gracefully in the future
//            joinSSEJobs()

            val publisherThread = stopZMQPublisher()
            applicationEngine.stop(0, 5000)

            publisherThread.join()
            appThread.join()
        }
    }
}