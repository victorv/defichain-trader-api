package com.trader.defichain.util

fun err(vararg context: Pair<String, Any>): String {
    val builder = java.lang.StringBuilder()
    builder.append("check failed, context:")
    for (entry in context) {
        builder.append("\n  -${entry.first}: ${entry.second}")
    }
    return builder.toString()
}