resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"

libraryDependencies ++= Seq(
  "org.geotools" % "gt-wps" % "8.0-RC1",
  "org.geoscript" %% "geoscript" % "0.8.0")
