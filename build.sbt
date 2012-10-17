resolvers ++= Seq(
  "osgeo" at "http://download.osgeo.org/webdav/geotools/",
  "opengeo" at "http://repo.opengeo.org/")

libraryDependencies += "org.geotools" % "gt-wps" % "8.1"
