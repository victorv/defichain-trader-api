package com.trader.defichain.util

import java.math.BigDecimal
import java.math.RoundingMode
import java.sql.ResultSet

private fun getColumnIndexForLabel(resultSet: ResultSet, columnLabel: String): Int {
    for (i in 1..resultSet.metaData.columnCount) {
        val labelAtIndex = resultSet.metaData.getColumnLabel(i)
        if (columnLabel == labelAtIndex) return i
    }
    throw IllegalArgumentException("Unable to find column with label: $columnLabel")
}
fun <T> ResultSet.get(columnLabel: String): T {
    val columnIndex = getColumnIndexForLabel(this, columnLabel)
    val value = this.getObject(columnIndex)
    if (value is BigDecimal) {
        return value.toDouble() as T
    }
    return value as T
}

fun ByteArray.toHex2(): String =
    asUByteArray().joinToString("") { it.toString(radix = 16).padStart(2, '0') }

fun BigDecimal.floor() = this.setScale(8, RoundingMode.FLOOR).toDouble()

fun BigDecimal.up() = this.setScale(8, RoundingMode.UP).toDouble()

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