package com.mindcandy.waterfall.app

import org.apache.commons.daemon._

/*
 These three elements all need to be extended, but should provide a framework for the core of an application.
 This includes support for JSVC startup and shutdown as well.
 */

trait ApplicationLifecycle {
  def start(): Unit
  def stop(): Unit
}

abstract class AbstractApplicationDaemon extends Daemon {
  def application: ApplicationLifecycle

  def init(daemonContext: DaemonContext) {}

  def start() = application.start()

  def stop() = application.stop()

  def destroy() = application.stop()
}

abstract class ApplicationRunner extends App {
  def createApplication(): AbstractApplicationDaemon

  private[this] var cleanupAlreadyRun: Boolean = false

  def cleanup() {
    val previouslyRun = cleanupAlreadyRun
    cleanupAlreadyRun = true
    if (!previouslyRun) application.stop()
  }

  val application = createApplication()
  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      cleanup()
    }
  }))
  application.start()
  Console.readLine()
  cleanup()
  sys.exit(0)
}

