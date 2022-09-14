package com.trader.defichain.telegram

import com.trader.defichain.appServerConfig
import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.testPoolSwap
import com.trader.defichain.zmq.newZQMBlockChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.math.BigDecimal
import java.math.RoundingMode
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.coroutines.CoroutineContext

private val accounts = CopyOnWriteArrayList<Account>()
private val blockHashChannel = newZQMBlockChannel()
suspend fun manageAccounts(coroutineContext: CoroutineContext) {
    while (coroutineContext.isActive) {
        try {
            manageAccounts()
        } catch (e: Throwable) {
            e.printStackTrace()
        } finally {
            delay(250)
        }
    }
}

private suspend fun manageAccounts() {
    blockHashChannel.receive()
    for (account in accounts) {
        try {
            for (poolSwap in account.poolSwaps) {
                while (poolSwap.estimates.size >= 100) {
                    poolSwap.estimates.removeFirst()
                }

                val previousEstimate = poolSwap.estimate
                val estimate = testPoolSwap(poolSwap).estimate
                val isDifferent = poolSwap.updateEstimate(estimate)
                if (isDifferent) {
                    poolSwap.estimates.add(Pair(System.currentTimeMillis(), estimate))
                    account.write()

                    val desiredResult = poolSwap.desiredResult!!
                    val profit =
                        BigDecimal(100.0 / desiredResult * estimate - 100.0).setScale(4, RoundingMode.FLOOR)
                            .toPlainString()
                    val swap = "${poolSwap.amountFrom} ${poolSwap.tokenFrom} to ${poolSwap.tokenTo}\nprofit: $profit%"
                    if (previousEstimate >= desiredResult && estimate < desiredResult) {
                        poolSwap.bestEstimate = estimate
                        sendTelegramMessage(account.chatID, "No longer profitable!\n$swap")
                    } else if (previousEstimate < desiredResult && estimate >= desiredResult) {
                        sendTelegramMessage(account.chatID, "Now profitable!\n$swap")
                    } else if (estimate > previousEstimate && estimate >= desiredResult) {
                        if (estimate > poolSwap.bestEstimate) {
                            poolSwap.bestEstimate = estimate
                            sendTelegramMessage(account.chatID, "Profit increased!\n$swap")
                        }
                    }
                }
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    }
}

fun loadAccounts() {
    Files.list(Paths.get(appServerConfig.accountsRoot)).forEach {
        val contents = Files.readAllBytes(it).decodeToString()
        val account = Json.decodeFromString<Account>(contents)
        accounts.add(account)
        account.write()
    }
}

suspend fun createAccountId(): String {
    val accountID = UUID.randomUUID().toString()
    unapprovedAccountsIDQueue.send(accountID)
    return accountID
}

suspend fun getAccountByChatId(chatId: Long): Account? {
    val account = accounts.find { it.chatID == chatId } ?: return null
    val isValid = account.checkValidity()
    return if (isValid) account else null
}

suspend fun approveAccount(newAccount: Account) {
    var updated = false
    for ((index, account) in accounts.withIndex()) {
        // existing account ID was resubmitted via defichain-trader.com Telegram bot
        if (account.id == newAccount.id) {
            updated = true
            val updatedAccount = newAccount.copy(poolSwaps = account.poolSwaps)
            updatedAccount.write()
            accounts[index] = updatedAccount
            break
        }
    }

    if (!updated) {
        newAccount.write()
        accounts.add(newAccount)
    }
    sendTelegramMessage(newAccount.chatID, "Login Code: ${newAccount.chatID}")
}

@kotlinx.serialization.Serializable
data class AccountData(
    val chatID: Long,
    val poolSwaps: List<PoolSwap>,
)

@kotlinx.serialization.Serializable
data class Account(
    val id: String,
    val userID: Long,
    val chatID: Long,
    var poolSwaps: List<PoolSwap>,
) {
    private var disabled = false

    suspend fun checkValidity(): Boolean {
        var chat: TelegramChat? = null
        try {
            chat = getChat(chatID)
        } finally {
            disabled = chat == null
        }
        return !disabled
    }

    fun getData() = AccountData(chatID = chatID, poolSwaps = poolSwaps)

    fun write() {
        val path = Paths.get(appServerConfig.accountsRoot).resolve("$userID.json")
        val json = Json.encodeToString(this.copy(poolSwaps = poolSwaps.map {
            it.copy(estimates = ArrayList())
        }))
        Files.write(path, json.toByteArray(StandardCharsets.UTF_8))
    }

    fun update(poolSwaps: List<PoolSwap>) {
        this.poolSwaps = poolSwaps.map {
            val existingSwap = this.poolSwaps.find { existingSwap ->
                it.amountFrom == existingSwap.amountFrom
                        && it.tokenFrom == existingSwap.tokenFrom
                        && it.tokenTo == existingSwap.tokenTo
            }
            if (existingSwap != null) {
                return@map it.copy(
                    estimates = existingSwap.estimates,
                    estimate = existingSwap.estimate,
                    bestEstimate = existingSwap.bestEstimate,
                )
            }
            it
        }

        write()
    }
}