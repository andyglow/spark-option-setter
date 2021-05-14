import xerial.sbt.Sonatype._
import ReleaseTransformations._
import scala.sys.process._

// https://github.com/xerial/sbt-sonatype/issues/71
ThisBuild / publishTo := sonatypePublishTo.value

ThisBuild / versionScheme := Some("pvp")

organization := "com.github.andyglow"

homepage := Some(new URL("http://github.com/andyglow/spark-option-setter"))

startYear := Some(2020)

organizationName := "andyglow"

scalaVersion := "2.13.5"

crossScalaVersions := Seq("2.13.5")

scalacOptions ++= {
  val options = Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture",
    "-language:higherKinds")

  // WORKAROUND https://github.com/scala/scala/pull/5402
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) => options.map {
      case "-Xlint"               => "-Xlint:-unused,_"
      case "-Ywarn-unused-import" => "-Ywarn-unused:imports,-patvars,-privates,-locals,-params,-implicits"
      case other                  => other
    }
    case _             => options
  }
}

Compile / doc / scalacOptions ++= Seq(
  "-groups",
  "-implicits",
  "-no-link-warnings")

licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

sonatypeProfileName := "com.github.andyglow"

publishMavenStyle := true

sonatypeProjectHosting := Some(
  GitHubHosting(
    "andyglow",
    "spark-option-setter",
    "andyglow@gmail.com"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/andyglow/spark-option-setter"),
    "scm:git@github.com:andyglow/spark-option-setter.git"))

developers := List(
  Developer(
    id    = "andyglow",
    name  = "Andriy Onyshchuk",
    email = "andyglow@gmail.com",
    url   = url("https://ua.linkedin.com/in/andyglow")))

releaseCrossBuild := true

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _), enableCrossBuild = true),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _), enableCrossBuild = true),
  pushChanges)

lazy val sparkV = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"             % sparkV     % Provided,
  "org.apache.spark"  %% "spark-streaming"        % sparkV     % Provided,
  "org.apache.spark"  %% "spark-sql"              % sparkV     % Provided,
  "org.mockito"        % "mockito-core"           % "3.10.0"    % Test,
  "org.scalatest"     %% "scalatest"              % "3.2.9"    % Test,
  "org.scalatestplus" %% "scalatestplus-mockito"  % "1.0.0-M2" % Test)
