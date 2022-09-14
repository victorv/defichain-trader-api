package com.trader.defichain.telegram

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import java.util.*
import kotlin.coroutines.CoroutineContext

val unapprovedAccountsIDQueue = Channel<String>(100, BufferOverflow.DROP_OLDEST)
private var unapprovedAccountIdentifiers = TreeSet<String>()

suspend fun approveNewAccounts(coroutineContext: CoroutineContext) {
    var offset = 0L
    while (coroutineContext.isActive) {
        try {
            val update = getTelegramUpdates(offset)
            val lastUpdateID = update.lastUpdateID()
            if (lastUpdateID != 0L) {
                offset = lastUpdateID + 1
            }

            val updates = update.getValidUpdates()

            if (updates.isNotEmpty()) {
                collectUnapprovedAccounts()
                approveAccounts(updates)
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        } finally {
            delay(1000)
        }
    }
}

private suspend fun approveAccounts(updates: List<ValidTelegramUpdate>) {
    for (update in updates) {
        val command = update.message.text.split(" ")
        if (command.size != 2) {
            continue
        }

        val accountID = command.last()
        if (unapprovedAccountIdentifiers.contains(accountID)) {
            unapprovedAccountIdentifiers.remove(accountID)

            val account = Account(
                id = accountID,
                poolSwaps = emptyList(),
                chatID = update.message.chat.id,
                userID = update.message.from.id
            )
            approveAccount(account)
        }
    }
}

private suspend fun collectUnapprovedAccounts() {
    if (unapprovedAccountsIDQueue.isEmpty) {
        return
    }

    val newIdentifiers = TreeSet<String>()
    newIdentifiers.addAll(unapprovedAccountIdentifiers)
    while (!unapprovedAccountsIDQueue.isEmpty) {
        newIdentifiers.add(unapprovedAccountsIDQueue.receive())
    }
    newIdentifiers.removeIf { newIdentifiers.size > 100 }

    unapprovedAccountIdentifiers = newIdentifiers
}