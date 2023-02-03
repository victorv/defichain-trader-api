package com.trader.defichain.util

val version = "^v\\d$".toRegex()
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