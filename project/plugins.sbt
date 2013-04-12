resolvers += Resolver.url("sbt-plugin-releases",
  new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

resolvers ++= Seq(
	  "Ansvia public releases repo" at "http://scala.repo.ansvia.com/releases"
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.3")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.1.0")

