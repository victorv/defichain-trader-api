package com.trader.defichain.util

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