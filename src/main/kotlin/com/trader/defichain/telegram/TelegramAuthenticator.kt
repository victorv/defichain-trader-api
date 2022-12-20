package com.trader.defichain.telegram

import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlin.coroutines.CoroutineContext

suspend fun approveNewNotifications(coroutineContext: CoroutineContext) {
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
                approveNotifications(updates)
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        } finally {
            delay(1000)
        }
    }
}

private suspend fun approveNotifications(updates: List<ValidTelegramUpdate>) {
    for (update in updates) {
        val command = update.message.text.split(" ")
        if (command.size == 2) {
            val uuid = command.last()
            if (command[0] == "delete") {
                for (notification in notifications) {
                    if (notification.uuid == uuid) {
                        notification.delete()
                        break
                    }
                }
            } else {
                approveNotification(uuid, update.message.chat.id)
            }
        }
    }
}