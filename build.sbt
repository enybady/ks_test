lazy val ks_test = (project in file(".")).
  settings(
    organization       := "com.vita",
    scalaVersion       := "2.12.8",
    name               := "kstest",
    mainClass          :=  Some("kstest.Main"),
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    libraryDependencies ++= {
      val kafkaVersion         = "2.2.0"
      Seq(
        "org.apache.kafka"           %%  "kafka"                        % kafkaVersion,
        "org.apache.kafka"           %   "kafka-streams"                % kafkaVersion,
        "org.apache.kafka"           %%  "kafka-streams-scala"          % kafkaVersion,
        "org.scalatest"              %%  "scalatest"                    % "3.0.8" % "test",
      )
    }
  )
