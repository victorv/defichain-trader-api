package com.trader.defichain.dex

import com.trader.defichain.util.containsSwapPath

private val emptyPath = emptyList<List<Int>>()
private val invalidConnection = Connection(-1, TokenNode(-1))

private class Connection(val poolId: Int, val token: TokenNode)
private class TokenNode(val tokenId: Int) {

    val connected = HashMap<Int, Connection>()

    fun findPaths(
        toTokenId: Int,
        path: Array<Connection>,
        depth: Int,
        paths: MutableList<List<Int>>,
    ) {
        for (connection in connected.values) {
            val connectedToken = connection.token
            if (path.copyOfRange(0, depth).any { it.token == connectedToken || it.poolId == connection.poolId }) {
                continue
            }
            path[depth] = connection

            if (connectedToken.tokenId == toTokenId) {
                paths.add(path.copyOfRange(0, depth + 1).map { it.poolId }.toList())
            } else if (depth <= 1) { // consensus dictates that you can only swap through 3 pools; 3rd pool is tried at depth 2
                connectedToken.findPaths(toTokenId, path, depth + 1, paths)
            }
        }
    }
}

class PoolTree {

    private val tokens = HashMap<Int, TokenNode>()

    fun addPool(poolId: Int, poolPair: PoolPair) {
        val tokenA = tokens.getOrPut(poolPair.idTokenA) { TokenNode(poolPair.idTokenA) }
        val tokenB = tokens.getOrPut(poolPair.idTokenB) { TokenNode(poolPair.idTokenB) }
        tokenA.connected[poolPair.idTokenB] = Connection(poolId, tokenB)
        tokenB.connected[poolPair.idTokenA] = Connection(poolId, tokenA)
    }

    fun getSwapPaths(tokenFromSymbol: String, tokenToSymbol: String): List<List<Int>> {
        val fromId = getTokenId(tokenFromSymbol) ?: return emptyPath
        val toId = getTokenId(tokenToSymbol) ?: return emptyPath

        val tokenFrom = tokens[fromId] ?: return emptyPath
        val tokenTo = tokens[toId] ?: return emptyPath

        val paths = mutableListOf<List<Int>>()
        tokenFrom.findPaths(tokenTo.tokenId, Array(8) { invalidConnection }, 0, paths)

        paths.sortBy { it.size }
        return paths.filter { path -> paths.none { path.containsSwapPath(it) } }
    }
}