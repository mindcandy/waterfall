import sbt._
import sbt.Keys._
import sbtbuildinfo.Plugin._
import sbtrelease.ReleasePlugin._
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform._
import scoverage.ScoverageSbtPlugin


object WaterfallBuild extends Build {
  val akkaVersion = "2.3.2"
  val sprayVersion = "1.3.1"
  val astyanaxVersion = "1.56.48"

  lazy val basicDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6",
    libraryDependencies += "com.typesafe.slick" %% "slick" % "2.1.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13",
    libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "1.0",
    libraryDependencies += "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
    libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.3",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.6.0",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.6.1",
    libraryDependencies += "org.apache.commons" % "commons-vfs2" % "2.0",
    libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1",
    libraryDependencies += "commons-daemon" % "commons-daemon" % "1.0.5",
    libraryDependencies += "com.netflix.astyanax" % "astyanax-cassandra" % astyanaxVersion exclude ("javax.servlet", "servlet-api") exclude("org.slf4j", "slf4j-log4j12"),
    libraryDependencies += "com.netflix.astyanax" % "astyanax-thrift" % astyanaxVersion exclude ("javax.servlet", "servlet-api") exclude("org.slf4j", "slf4j-log4j12"),
    libraryDependencies += "com.netflix.astyanax" % "astyanax-recipes" % astyanaxVersion exclude ("javax.servlet", "servlet-api") exclude("org.slf4j", "slf4j-log4j12")
  )
    
  lazy val sprayDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies += "io.spray"             %   "spray-can"     % sprayVersion,
    libraryDependencies += "io.spray"             %   "spray-client"  % sprayVersion,
    libraryDependencies += "io.spray"             %   "spray-routing" % sprayVersion,
    libraryDependencies += "io.spray"             %   "spray-testkit" % sprayVersion  % "test,it",
    libraryDependencies += "com.typesafe.akka"    %%  "akka-actor"    % akkaVersion,
    libraryDependencies += "com.typesafe.akka"    %%  "akka-testkit"  % akkaVersion   % "test,it",
    libraryDependencies += "io.argonaut"          %%  "argonaut"      % "6.0.4",
    libraryDependencies += "org.quartz-scheduler" %   "quartz"        % "2.2.1")

  lazy val testDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test,it",
    libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5" % "test,it",
    libraryDependencies += "org.specs2" %% "specs2" % "2.3.8" % "test,it",
    libraryDependencies += "junit" % "junit" % "4.11" % "test,it",
    libraryDependencies += "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test,it",
    libraryDependencies += "com.github.simplyscala" %% "simplyscala-server" % "0.5" % "test,it",
    libraryDependencies += "com.h2database" % "h2" % "1.4.180" % "test,it")

  lazy val formattingSettings = FormattingPreferences()
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, false)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 40)
    .setPreference(CompactControlReadability, false)
    .setPreference(CompactStringConcatenation, false)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(FormatXml, true)
    .setPreference(IndentLocalDefs, false)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(IndentSpaces, 2)
    .setPreference(IndentWithTabs, false)
    .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
    .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, false)
    .setPreference(PreserveSpaceBeforeArguments, false)
    .setPreference(PreserveDanglingCloseParenthesis, true)
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(SpaceBeforeColon, false)
    .setPreference(SpaceInsideBrackets, false)
    .setPreference(SpaceInsideParentheses, false)
    .setPreference(SpacesWithinPatternBinders, true)

  lazy val itRunSettings = Seq(
    fork in IntegrationTest := true,
    connectInput in IntegrationTest := true
  )

  def vcsNumber: String = {
    val vcsBuildNumber = System.getenv("BUILD_VCS_NUMBER")
    if (vcsBuildNumber == null) "" else vcsBuildNumber
  }


  lazy val waterfall = Project(
    id = "waterfall",
    base = file("."),
    settings = Project.defaultSettings ++ basicDependencies ++ releaseSettings ++ defaultScalariformSettingsWithIt ++ itRunSettings ++ testDependencies ++ sprayDependencies ++ buildInfoSettings ++ Seq(
      name := "waterfall",
      organization := "com.mindcandy.waterfall",
      scalaVersion := "2.10.4",
      ScalariformKeys.preferences := formattingSettings,
      publishTo <<= (version) { version: String =>
        val repo = "http://artifactory.tool.mindcandy.com/artifactory/"
        val revisionProperty = if (!vcsNumber.isEmpty) ";revision=" + vcsNumber else ""
        val timestampProperty = ";build.timestamp=" + new java.util.Date().getTime
        val props = timestampProperty + revisionProperty
        if (version.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at repo + "libs-snapshot-local" + props)
        else
          Some("releases" at repo + "libs-release-local" + props)
      },
      ScoverageSbtPlugin.ScoverageKeys.highlighting := true,
      // build info
      sourceGenerators in Compile <+= buildInfo,
      buildInfoKeys := Seq[BuildInfoKey](name, version),
      buildInfoPackage := "com.mindcandy.waterfall.info"
    )
  ).configs( IntegrationTest )
   .settings( ScoverageSbtPlugin.instrumentSettings ++ Defaults.itSettings ++ Seq(unmanagedSourceDirectories in IntegrationTest <++= { baseDirectory { base => { Seq( base / "src/test/scala" )}}}) : _*)

}
