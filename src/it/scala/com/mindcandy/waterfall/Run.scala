package com.mindcandy.waterfall

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import com.mindcandy.waterfall.service.WaterfallApp

@RunWith(classOf[JUnitRunner])
class RunSpec extends Specification with Mockito {

    "Run" should {
      "run the app" in {
        WaterfallApp.createApplication().application.start
        success
      }
    }
}
