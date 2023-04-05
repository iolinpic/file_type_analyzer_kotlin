package analyzer

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class UnknownFileTypeException(filename: String) : Exception("$filename: Unknown file type")

data class PatternRecord(val priority: Int, val pattern: String, val type: String)

fun main(args: Array<String>) {
    if (args.size < 2) return
    val patterns = readPattern(args[1])
    val path = Paths.get(args[0])
    val workersManager = WorkersManager<PatternChecker>()
    if (Files.isDirectory(path)) {
        val files = Files.list(path).filter { !Files.isDirectory(it) }
        for (file in files) {
            val patternChecker = PatternChecker(file.toString(), patterns)
            workersManager.workers.add(patternChecker)
        }
    }
    workersManager.startAll()
    workersManager.joinAll()

}

fun readPattern(filename: String): List<PatternRecord> {
    val patterns = mutableListOf<PatternRecord>()
    try {
        File(filename).forEachLine {
            val splitedList = it.split(";")
            if (splitedList.size == 3) {
                patterns.add(
                    PatternRecord(
                        splitedList[0].toInt(),
                        splitedList[1].replace("\"", ""),
                        splitedList[2].replace("\"", "")
                    )
                )
            }
        }
    } catch (ex: Exception) {
        println(ex.message)
    }
    patterns.sortByDescending { it.priority }
    return patterns.toList()
}

class WorkersManager<T : Thread>() {
    val workers = mutableListOf<T>()
    fun startAll() {
        workers.forEach {
            it.start()
        }
    }

    fun joinAll() {
        workers.forEach {
            it.join()
        }
    }
}

class PatternMatcher(private val text: String, val pattern: PatternRecord) : Thread() {
    var success = false
    override fun run() {
        success = rabinKarpSearch(text, pattern.pattern)
    }

    private fun rabinKarpSearch(text: String, pattern: String): Boolean {
        val a = 117
        val m = 173961102589771L
        if (pattern.length > text.length) {
            return false
        }
        var patternHash: Long = 0
        var currSubstrHash: Long = 0
        var pow: Long = 1
        for (i in pattern.indices) {
            patternHash += pattern[i].code.toLong() * pow
            patternHash %= m
            currSubstrHash += text[text.length - pattern.length + i].code.toLong() * pow
            currSubstrHash %= m
            if (i != pattern.length - 1) {
                pow = pow * a % m
            }
        }
        for (i in text.length downTo pattern.length) {
            if (patternHash == currSubstrHash) {
                for (j in pattern.indices) {
                    if (text[i - pattern.length + j] != pattern[j]) {
                        break
                    }
                }
                return true
            }
            if (i > pattern.length) {
                currSubstrHash = (currSubstrHash - text[i - 1].code.toLong() * pow % m + m) * a % m
                currSubstrHash = (currSubstrHash + text[i - pattern.length - 1].code.toLong()) % m
            }
        }
        return false
    }
}

class PatternChecker(private val filename: String, private val patterns: List<PatternRecord>) : Thread() {
    override fun run() {
        try {
            var success = false
            File(filename).forEachLine {
                val workersManager = WorkersManager<PatternMatcher>()
                patterns.forEach { pattern ->
                    workersManager.workers.add(PatternMatcher(it, pattern))
                }
                workersManager.startAll()
                workersManager.joinAll()
                val result = workersManager.workers.firstOrNull { it.success }
                if (result != null) {
                    success = true
                    println("$filename: ${result.pattern.type}")
                    return@forEachLine
                }
            }
            if (!success) throw UnknownFileTypeException(filename)
        } catch (ex: Exception) {
            println(ex.message)
        }
    }
}