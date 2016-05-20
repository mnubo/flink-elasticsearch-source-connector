import scala.xml.Group
import com.typesafe.sbt.JavaVersionCheckPlugin.autoImport._

val flinkVersion = "1.0.3"

organization := "com.mnubo"

name := "flink-elasticsearch-source-connector"

crossPaths := true

scalaVersion := "2.10.6"

crossScalaVersions := Seq("2.10.6", "2.11.7")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-optimize", "-feature")

scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      Seq("-Ywarn-unused-import", "-Ywarn-unused")
    case _ =>
      Nil
  }
}

javacOptions ++= Seq("-target", "1.6", "-source", "1.6")

javaVersionPrefix in javaVersionCheck := Some{
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor <= 11 => "1.7"
    case _ => "1.8"
  }
}

libraryDependencies ++= Seq(
  "org.apache.flink"          %% "flink-scala"        % flinkVersion,

  "org.apache.flink"          %% "flink-clients"      % flinkVersion % "test",
  "org.elasticsearch"         %  "elasticsearch"      % "1.5.2" % "test",
  "org.scalatest"             %% "scalatest"          % "2.2.6" % "test"
)

homepage := Some(new URL("https://github.com/mnubo/flink-elasticsearch-source-connector"))

startYear := Some(2016)

licenses := Seq(("Apache-2.0", new URL("http://www.apache.org/licenses/LICENSE-2.0")))

pomExtra <<= (pomExtra, name, description) {(pom, name, desc) => pom ++ Group(
  <scm>
    <url>http://github.com/mnubo/flink-elasticsearch-source-connector</url>
    <connection>scm:git:git://github.com/mnubo/flink-elasticsearch-source-connector.git</connection>
  </scm>
  <developers>
    <developer>
      <id>jletroui</id>
      <name>Julien Letrouit</name>
      <url>http://julienletrouit.com/</url>
    </developer>
    <developer>
      <id>lemieud</id>
      <name>David Lemieux</name>
      <url>https://github.com/lemieud</url>
    </developer>
  </developers>
)}

resolvers ++= Seq(Opts.resolver.sonatypeSnapshots, Opts.resolver.sonatypeReleases)

crossVersion := CrossVersion.binary