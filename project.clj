(defproject com.github.csm/konserve-ddb-s3 "0.1.2-SNAPSHOT"
  :description "Konserve store atop DynamoDB and S3"
  :url "https://github.com/csm/konserve-ddb-s3"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [io.replikativ/konserve "0.6.0-SNAPSHOT" :exclusions [org.clojure/core.async]]
                 [com.cognitect.aws/api "0.8.352"]
                 [com.cognitect.aws/endpoints "1.1.11.632"]
                 [com.cognitect.aws/s3 "726.2.488.0"]
                 [com.cognitect.aws/dynamodb "746.2.533.0"]
                 [com.cognitect/anomalies "0.1.12"]
                 [org.lz4/lz4-java "1.6.0"]]
  :profiles {:test {:resource-paths ["test-resources"]
                    :dependencies [[ch.qos.logback/logback-classic "1.1.8"]
                                   [ch.qos.logback/logback-core "1.1.8"]
                                   [io.replikativ/superv.async "0.2.9"]
                                   [s4 "0.1.10-SNAPSHOT" :exclusions [org.clojure/tools.logging]]
                                   [com.amazonaws/DynamoDBLocal "1.11.477"
                                    :exclusions [com.fasterxml.jackson.core/jackson-core
                                                 org.eclipse.jetty/jetty-client
                                                 com.google.guava/guava
                                                 commons-logging]]
                                   [org.eclipse.jetty/jetty-server "9.4.15.v20190215"]
                                   [com.almworks.sqlite4java/libsqlite4java-osx "1.0.392" :extension "dylib"]
                                   [com.almworks.sqlite4java/libsqlite4java-linux-amd64 "1.0.392" :extension "so"]
                                   [org.slf4j/jul-to-slf4j "1.7.21"]
                                   [org.clojure/test.check "0.10.0"]]}
             :repl {:source-paths ["repl-src"]
                    :resource-paths ["test-resources"]}}
  :repositories [["aws-dynamodb-local" {:url "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"}]]
  :repl-options {:init-ns konserve-ddb-s3.repl})
