import com.trader.defichain.db.DBTX
import com.trader.defichain.db.insertAddress
import com.trader.defichain.db.insertTokens
import com.trader.defichain.db.insertVault
import com.trader.defichain.rpc.CustomTX
import com.trader.defichain.util.Future

private val template_insertAuctionBid = """
INSERT INTO auction_bid (tx_row_id, amount, token, vault, owner, index) 
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(tx_row_id) DO UPDATE SET
amount = ?,
token = ?,
vault = ?,
owner = ?,
index = ?
""".trimIndent()

fun DBTX.auctionBid(
    txRowID: Future<Long>,
    auctionBid: CustomTX.AuctionBid,
) = doLater {
    val (amount, tokenID) = auctionBid.amount()

    insertTokens(tokenID)

    val vault = insertVault(auctionBid.vaultID)
    val owner = insertAddress(auctionBid.from)

    connection.prepareStatement(template_insertAuctionBid).use {
        it.setLong(1, txRowID.get())
        it.setDouble(2, amount)
        it.setInt(3, tokenID)
        it.setLong(4, vault)
        it.setLong(5, owner)
        it.setInt(6, auctionBid.index)

        it.setDouble(7, amount)
        it.setInt(8, tokenID)
        it.setLong(9, vault)
        it.setLong(10, owner)
        it.setInt(11, auctionBid.index)

        check(it.executeUpdate() <= 1)
    }
}

// Record(type=AuctionBid, valid=true, results={"vaultId":"9c04815b3876e7467fe3aa163f3a2e1621b1428afbfabc9784065ebc192daffd","index":0,"from":"df1q9eptwy20dwymdpwzdv4fpwjwvn26vu9ctn3vke","amount":"0.00173551@26"})
