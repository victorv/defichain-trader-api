package com.trader.defichain.util

import java.math.BigDecimal
import java.math.RoundingMode
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

private val placeholder = "(?<!:):[a-z]+(_[a-z]+)*".toRegex()

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

fun Connection.prepareStatement(sql: String, parameters: Map<String, Any?>): PreparedStatement {
    val placeholders = placeholder.findAll(sql)
        .mapIndexed { index, match -> Pair(index + 1, match.value) }
        .associateWith {
            parameters.getValue(it.second.substring(1))
        }

    val sqlNativePlaceholders = placeholder.replace(sql, "?")
    val statement = this.prepareStatement(sqlNativePlaceholders)
    try {
        for ((placeholder, value) in placeholders) {
            if (value is SQLValue) {
                if(value.type == Types.ARRAY == value.value == null) {
                    val array = this.createArrayOf(value.arrayType, arrayOf(-1))
                    statement.setArray(placeholder.first, array)
                } else {
                    statement.setObject(placeholder.first, value.value, value.type)
                }
            } else {
                statement.setObject(placeholder.first, value)
            }
        }
    } catch (e: Throwable) {
        try {
            statement.close()
        } finally {
            throw e
        }
    }
    return statement
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

data class SQLValue(val value: Any?, val type: Int, val arrayType: String? = null)