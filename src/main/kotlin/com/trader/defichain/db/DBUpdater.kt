package com.trader.defichain.db

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import org.postgresql.ds.PGSimpleDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.coroutines.CoroutineContext

private val logger = LoggerFactory.getLogger("DBUpdater")
private val transactions = Channel<DBTX>(5)

private val pool = createWritableDataSource()

private fun createWritableDataSource(): PGSimpleDataSource {
    val connectionPool = PGSimpleDataSource()
    connectionPool.isReadOnly = false
    connectionPool.databaseName = "trader"
    connectionPool.user = "postgres"
    connectionPool.password = "postgres"
    return connectionPool
}

private fun createWriteableConnection(): Connection {
    val connection = pool.connection
    connection.autoCommit = false
    connection.isReadOnly = false
    return connection
}

private fun useOrReplace(connection: Connection): Connection {
    if (!connection.isValid(1000)) {
        logger.warn("Connection $connection is no longer valid and will be replaced")
        try {
            connection.close()
        } catch (e: Throwable) {
            logger.warn("Suppressed while closing invalid connection", e)
        }
        return createWriteableConnection()
    }
    return connection
}

suspend fun updateDatabase(coroutineContext: CoroutineContext) {
    var connection = createWriteableConnection()
    while (coroutineContext.isActive) {
        var dbtx = transactions.receive()
        dbtx.connection = connection
        try {
            dbtx.executeStatements()
            connection.commit()

            logger.info("Committed transaction: ${dbtx.description}")
        } catch (t: Throwable) {
            try {
                connection.rollback()
            } catch (rollbackException: Throwable) {
                t.addSuppressed(rollbackException)
            }

            try {
                connection = useOrReplace(connection)
            } catch (e: Throwable) {
                t.addSuppressed(e)
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