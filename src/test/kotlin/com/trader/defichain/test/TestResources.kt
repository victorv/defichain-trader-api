package com.trader.defichain.test

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val testResourcesPath = "src/test/resources"

fun getTestResourcesDirectory(): Path {
    val projectDir = Paths.get(System.getProperty("user.dir"))
    check(Files.exists(projectDir) && Files.isDirectory(projectDir)) { "File is not a directory: `$projectDir`" }
    val testResourcesDir = projectDir.resolve(testResourcesPath)
    check(Files.exists(testResourcesDir) && Files.isDirectory(testResourcesDir)) {
        "Directory `$testResourcesPath` is missing in `$projectDir`"
    }
    return testResourcesDir
}