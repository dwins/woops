resolvers ++= Seq(
  "osgeo" at "http://download.osgeo.org/webdav/geotools/",
  "opengeo" at "http://repo.opengeo.org/")

libraryDependencies ++= Seq(
  "org.geotools" % "gt-wps" % "8-SNAPSHOT",
  "org.geoscript" %% "geoscript" % "0.8.0")
