package com.mindcandy.waterfall.service

import org.specs2.ScalaCheck
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification
import spray.http.{ MediaTypes, StatusCodes }
import spray.routing.Directives
import spray.testkit.Specs2RouteTest

class UserInterfaceRouteSpec extends Specification with ScalaCheck with Grouped with Specs2RouteTest with Directives {

  val route = UserInterfaceRoute().route

  def is = s2"""
  UserInterfaceRoute test
  ==============================================================================
    get / returns the index page ${getUIByRootPath}
    get /ui returns the index page ${getUIByUiPath}
    get /assets/*.css returns css assets ${getUIAssests}
    get /assets/*.map returns json assets ${getJsonMapAssests}
  """

  def getUIByRootPath = Get("/") ~> route ~> check {
    status must be_==(StatusCodes.OK) and
      (response.entity.asString must contain("<div ng-view></div>")) and
      (mediaType must be_==(MediaTypes.`text/html`))
  }

  def getUIByUiPath = Get("/ui") ~> route ~> check {
    status must be_==(StatusCodes.OK) and
      (response.entity.asString must contain("<div ng-view></div>")) and
      (mediaType must be_==(MediaTypes.`text/html`))
  }

  def getUIAssests = Get("/assets/css/main.css") ~> route ~> check {
    status must be_==(StatusCodes.OK) and
      (mediaType must be_==(MediaTypes.`text/css`))
  }

  def getJsonMapAssests = Get("/assets/bootstrap/css/bootstrap.css.map") ~> route ~> check {
    status must be_==(StatusCodes.OK) and
      (mediaType must be_==(MediaTypes.`application/json`))
  }
}
