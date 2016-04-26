enablePlugins(MnuboLibraryPlugin)

name := "flink-elasticsearch-source-connector"

crossPaths := true

scalaVersion := "2.10.6"

crossScalaVersions := Seq("2.10.6", "2.11.7")

val flinkVersion = "1.0.2"

libraryDependencies ++= Seq(
  "org.apache.flink"          %% "flink-scala"        % flinkVersion,

  "org.apache.flink"          %% "flink-clients"      % flinkVersion % "test",
  "org.elasticsearch"         %  "elasticsearch"      % "1.5.2" % "test",
  "org.scalatest"             %% "scalatest"          % "2.2.6" % "test"
)
