package com.mindcandy.waterfall

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import com.mindcandy.waterfall.service.ApplicationDaemon

@RunWith(classOf[JUnitRunner])
class RunSpec extends Specification with Mockito {
  val shouldRun = false

  "Run" should {
    "run the app" in {
      if (shouldRun) {
        ApplicationDaemon().start()
        Console.readLine()
      }
      success
    }
  }
}
