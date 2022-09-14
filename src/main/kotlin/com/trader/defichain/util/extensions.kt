package com.trader.defichain.util

import java.math.BigDecimal
import java.math.RoundingMode

fun ByteArray.toHex2(): String =
    asUByteArray().joinToString("") { it.toString(radix = 16).padStart(2, '0') }

fun BigDecimal.floor() = this.setScale(8, RoundingMode.FLOOR).toDouble()

fun BigDecimal.floorPlain(): String = this.setScale(8, RoundingMode.FLOOR).toPlainString()

fun <T> List<T>.containsSwapPath(other: List<T>): Boolean {
    if (this === other || other.isEmpty() || other.size > this.size) {
        return false
    }

    var prevIndex = -1
    for (value in other) {
        val indexOfValue = this.indexOf(value)
        if (indexOfValue <= prevIndex) {
            return false
        }
        prevIndex = indexOfValue
    }
    return true
}