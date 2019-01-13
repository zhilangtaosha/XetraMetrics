package com.acorns.techtest

import org.apache.commons.cli.{CommandLine, GnuParser, Option, Options}

class JobOptions(args: Array[String]) {
  private val options = new Options()
  addOptions(options)

  private var commandLine: CommandLine = new GnuParser().parse(options, args, true)

  val filePath: String = commandLine.getOptionValue("path")

  private def addOptions(jobOptions: Options): Unit = {
    val logTableNameOption = new Option("p", "path", true, "file path of resource data")
    logTableNameOption.setRequired(true)
    jobOptions.addOption(logTableNameOption)
  }
}