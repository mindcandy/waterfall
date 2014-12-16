package com.mindcandy.waterfall

import com.mindcandy.waterfall.service.ApplicationDaemon
import com.typesafe.scalalogging.slf4j.Logging

/*
    Runs the waterfall daemon.

    To get this working in IntelliJ I add the it:resources folder to the waterfall's classpath resources folders under Project Structure
  */
object RunWaterfall extends App with Logging {

  logger.error("start")

  ApplicationDaemon().start()
  Console.readLine()
  ApplicationDaemon().stop()

  logger.error("end")

}
