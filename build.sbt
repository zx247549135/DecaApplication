name := "DecaApplication"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases/"
)
    