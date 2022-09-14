package com.trader.defichain.test

import com.trader.defichain.App
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMsg

var publisherSocket: ZMQ.Socket? = null
private var publisherThread: Thread? = null
private var run = true

fun publishZMQMessage() {
    val publisher = publisherSocket
    check(publisher != null)

    val message = ZMsg()
    message.add("hashblock")
    message.add(byteArrayOf(1, 2, 3, 4))
    message.send(publisher)
}

fun stopZMQPublisher(): Thread {
    val thread = publisherThread
    check(thread != null)

    run = false
    thread.interrupt()
    return thread
}

fun startZMQPublisher() {
    val thread = Thread {
        ZContext().use { context ->
            context.createSocket(SocketType.PUB).use { publisher ->
                publisherSocket = publisher

                publisher.bind("tcp://127.0.0.1:29500")

                while (run && !Thread.currentThread().isInterrupted && !App.zmqSubscriberConnected) {
                    Thread.sleep(100)
                }

                while (run) {
                    try {
                        Thread.sleep(100)
                    } catch (e: InterruptedException) {
                        break
                    }
                }
            }
        }
    }
    publisherThread = thread
    thread.start()
}