resolvers += "Mnubo release repository" at "http://artifactory.mtl.mnubo.com:8081/artifactory/libs-release-local/"

val latestPluginVersion = Using.urlInputStream(new URL("http://artifactory.mtl.mnubo.com:8081/artifactory/libs-release-local/com/mnubo/mnubo-sbt-plugin_2.10_0.13/maven-metadata.xml")) { stream =>
 """<latest>([\d\.]+)</latest>""".r.findFirstMatchIn(IO.readStream(stream)).get.group(1)
}

addSbtPlugin("com.mnubo" % "mnubo-sbt-plugin" % latestPluginVersion)
