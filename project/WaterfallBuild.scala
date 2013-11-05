import sbt._
import sbt.Keys._
import sbtrelease._
import sbtrelease.ReleasePlugin._
import sbtrelease.ReleasePlugin.ReleaseKeys._

object WaterfallBuild extends Build {

  lazy val basicDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies += "com.typesafe.slick" %% "slick" % "1.0.1",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13",
    libraryDependencies += "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
    libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.3",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.6.0",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.6.1",
    libraryDependencies += "org.apache.commons" % "commons-vfs2" % "2.0",
    libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1")

  lazy val testDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
    libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5" % "test",
    libraryDependencies += "org.specs2" %% "specs2" % "2.2.3" % "test",
    libraryDependencies += "junit" % "junit" % "4.11" % "test",
    libraryDependencies += "postgresql" % "postgresql" % "9.1-901.jdbc4",
    libraryDependencies += "com.github.simplyscala" %% "simplyscala-server" % "0.5")

  def vcsNumber: String = {
    val vcsBuildNumber = System.getenv("BUILD_VCS_NUMBER")
    if (vcsBuildNumber == null) "" else vcsBuildNumber
  }

  lazy val waterfall = Project(
    id = "waterfall",
    base = file("."),
    settings = Project.defaultSettings ++ basicDependencies ++ releaseSettings ++ testDependencies ++ Seq(
      name := "waterfall",
      organization := "com.mindcandy",
      scalaVersion := "2.10.2",
      publishTo <<= (version) { version: String =>
        val repo = "http://artifactory.tool.mindcandy.com/artifactory/"
        val revisionProperty = if (!vcsNumber.isEmpty) ";revision=" + vcsNumber else ""
        val timestampProperty = ";build.timestamp=" + new java.util.Date().getTime
        val props = timestampProperty + revisionProperty
        if (version.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at repo + "libs-snapshot-local" + props)
        else
          Some("releases" at repo + "libs-release-local" + props)
      }
      ))
}
