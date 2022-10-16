package com.trader.defichain.dex

import com.trader.defichain.util.floor
import kotlinx.serialization.Contextual
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal

private val PRECISION = BigDecimal(10000)

object TokenIDSerializer : KSerializer<Int> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("TokenID", PrimitiveKind.INT)
    override fun serialize(encoder: Encoder, value: Int) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): Int {
        return decoder.decodeString().toInt()
    }
}

@kotlinx.serialization.Serializable
data class PoolPair(
    val symbol: String,
    val status: Boolean,
    @Serializable(with = TokenIDSerializer::class)
    val idTokenA: Int,
    @Serializable(with = TokenIDSerializer::class)
    val idTokenB: Int,
    val dexFeeInPctTokenA: Double? = null,
    val dexFeeOutPctTokenA: Double? = null,
    val dexFeeOutPctTokenB: Double? = null,
    val dexFeeInPctTokenB: Double? = null,
    val commission: Double? = null,
    val reserveA: Double,
    val reserveB: Double,
    val tradeEnabled: Boolean,
) {
    @Contextual
    var modifiedReserveA = BigDecimal(reserveA)

    @Contextual
    var modifiedReserveB = BigDecimal(reserveB)

    @Contextual
    val initialReserveA = BigDecimal(reserveA)

    @Contextual
    val initialReserveB = BigDecimal(reserveB)

    fun getPriceInfo(reserveA: BigDecimal, reserveB: BigDecimal) = PriceInfo(
        forwardPrice = (reserveA * PRECISION / reserveB).toDouble(),
        backwardPrice = (reserveB * PRECISION / reserveA).toDouble()
    )

    fun getPriceChange(priceInfo1: PriceInfo, priceInfo2: PriceInfo) = PriceInfo(
        forwardPrice = 100.0 / priceInfo1.forwardPrice * priceInfo2.forwardPrice - 100.0,
        backwardPrice = 100.0 / priceInfo1.backwardPrice * priceInfo2.backwardPrice - 100.0,
    )

    private fun getReserveFrom(forward: Boolean) = if (forward) modifiedReserveA else modifiedReserveB
    private fun getReserveTo(forward: Boolean) = if (forward) modifiedReserveB else modifiedReserveA

    fun swap(tokenFrom: String, amountFrom: BigDecimal): Pair<PoolSwapExplained, BigDecimal> {
        val (symbolA, symbolB) = symbol.split("-")
        val forward = symbolA == tokenFrom
        val reserveF = getReserveFrom(forward)
        val reserveT = getReserveTo(forward)

        var amount = amountFrom
        var commissionPayed: BigDecimal? = null
        if (commission != null) {
            commissionPayed = amountFrom * BigDecimal(commission)
            amount -= commissionPayed
        }

        var inFeePct: Double? = null
        var inFeePayed: BigDecimal? = null
        if (tokenFrom == symbolA && dexFeeInPctTokenA != null) {
            inFeePct = dexFeeInPctTokenA
            inFeePayed = amount * BigDecimal(dexFeeInPctTokenA)
            amount -= inFeePayed
        } else if (tokenFrom == symbolB && dexFeeInPctTokenB != null) {
            inFeePct = dexFeeInPctTokenB
            inFeePayed = amount * BigDecimal(dexFeeInPctTokenB)
            amount -= inFeePayed
        }

        val before = getPriceInfo(modifiedReserveA, modifiedReserveB)

        var result = reserveT - (reserveT * reserveF / (reserveF + amount))
        if (forward) modifiedReserveA += amount else modifiedReserveB += amount
        if (forward) modifiedReserveB -= result else modifiedReserveA -= result

        val after = getPriceInfo(modifiedReserveA, modifiedReserveB)

        val priceChange = getPriceChange(before, after)

        var outFeePct: Double? = null
        var outFeePayed: BigDecimal? = null
        if (tokenFrom == symbolA && dexFeeOutPctTokenB != null) {
            outFeePct = dexFeeOutPctTokenB
            outFeePayed = result * BigDecimal(dexFeeOutPctTokenB)
            result -= outFeePayed
        } else if (tokenFrom == symbolB && dexFeeOutPctTokenA != null) {
            outFeePct = dexFeeOutPctTokenA
            outFeePayed = result * BigDecimal(dexFeeOutPctTokenA)
            result -= outFeePayed
        }

        val amountTo = result.floor()
        val tokenFrom = if (symbolA == tokenFrom) symbolA else symbolB
        val tokenTo = if (symbolA == tokenFrom) symbolB else symbolA
        val explanation = PoolSwapExplained(
            tokenFrom = tokenFrom,
            tokenTo = tokenTo,
            amountFrom = amountFrom.floor(),
            amountTo = amountTo,
            commissionPct = commission,
            commission = commissionPayed?.floor(),
            inFeePct = inFeePct,
            inFee = inFeePayed?.floor(),
            outFeePct = outFeePct,
            outFee = outFeePayed?.floor(),
            priceBefore = before,
            priceAfter = after,
            priceChange = priceChange,
            status = status,
            tradeEnabled = tradeEnabled,
            overflow = amountTo < 0.0,
        )
        return Pair(explanation, result)
    }
}