package com.trader.defichain.db

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.coroutines.CoroutineContext

private val logger = LoggerFactory.getLogger("DBUpdater")
private val transactions = Channel<DBTX>(1000)

suspend fun updateDatabase(coroutineContext: CoroutineContext) {
    var connection = createWriteableConnection()
    while (coroutineContext.isActive) {
        var dbtx = transactions.receive()
        dbtx.connection = connection
        try {
            connection = useOrReplace(connection)
            dbtx.executeStatements()
            connection.commit()

            logger.info("Committed transaction: ${dbtx.description}")
        } catch (t: Throwable) {
            try {
                connection.rollback()
            } catch (rollbackException: Throwable) {
                t.addSuppressed(rollbackException)
            }
            logger.error("Unable to commit transaction: ${dbtx.description}", t)
        }
    }
}

class DBTX(val description: String) {

    lateinit var connection: Connection
    private val actions = CopyOnWriteArrayList<suspend () -> Unit>()

    fun doLater(action: suspend () -> Unit) {
        actions.add(action)
    }

    suspend fun executeStatements() {
        for (action in actions) {
            action()
        }
    }

    suspend fun submit() {
        transactions.send(this)
    }
}