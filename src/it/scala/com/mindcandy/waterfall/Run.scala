package com.mindcandy.waterfall

import com.mindcandy.waterfall.service.ApplicationDaemon
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

/*
   In order to run this set shouldRun to true. Do not commit this change.
 */
@RunWith(classOf[JUnitRunner])
class RunSpec extends Specification {
  val shouldRun = true

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
