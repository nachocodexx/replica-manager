lazy val app = (project in file(".")).settings(
  name := "load-balancing",
  version := "0.1",
  scalaVersion := "2.13.6",
  libraryDependencies  ++= Dependencies(),
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)
