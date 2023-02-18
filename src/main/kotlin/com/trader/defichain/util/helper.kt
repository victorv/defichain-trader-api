package com.trader.defichain.util

import com.trader.defichain.dex.PoolSwap
import com.trader.defichain.dex.testPoolSwap
import java.math.BigDecimal

const val DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss"
val version = "^v\\d$".toRegex()

fun round(num: Double) = round(num.toString())
fun round(num: String): String {
    val d = num.toDouble()
    val decimalPlaces = if (d < 0.01) 8
    else 0.coerceAtLeast(6 - d.toInt().toString().length)
    val formatString = "%." + decimalPlaces + "f"
    return String.format(formatString, num.toDouble())
}

fun asUSDT(amountFrom: Double, tokenSymbol: String): String {
    var estimate = amountFrom
    if (amountFrom != 0.0 && tokenSymbol != "USDT" && tokenSymbol != "USDC") {
        estimate = testPoolSwap(
            PoolSwap(
                amountFrom = amountFrom,
                tokenFrom = tokenSymbol,
                tokenTo = "USDT",
                desiredResult = 1.0,
            )
        ).estimate
    }

    if (estimate < 0.01) {
        return "0"
    }
    if (estimate < 100.0) {
        return BigDecimal(estimate).floorPlain(2)
    }
    return BigDecimal(estimate).floorPlain(0)
}

fun List<String>.joinVersions(): List<String> {
    val joined = ArrayList<String>(this.size)
    for(value in this) {
        if (version.matches(value)) {
            joined[joined.lastIndex] = joined[joined.lastIndex] + "/" + value
        } else {
            joined.add(value)
        }
    }
    return joined
}

fun err(vararg context: Pair<String, Any>): String {
    val builder = java.lang.StringBuilder()
    builder.append("check failed, context:")
    for (entry in context) {
        builder.append("\n  -${entry.first}: ${entry.second}")
    }
    return builder.toString()
}

class Future<T> {

    private var v: T? = null
    fun set(v: T) {
        this.v = v
    }

    fun get(): T = v!!
}