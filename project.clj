(defproject com.github.csm/konserve-ddb-s3 "0.1.2-SNAPSHOT"
  :description "Konserve store atop DynamoDB and S3"
  :url "https://github.com/csm/konserve-ddb-s3"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [org.clojure/tools.logging "1.1.0"]
                 [io.replikativ/konserve "0.5.1"]
                 [io.replikativ/superv.async "0.2.9"]
                 ; AWS
                 [software.amazon.awssdk/s3 "2.13.41"]
                 [software.amazon.awssdk/dynamodb "2.13.41"]
                 [org.lz4/lz4-java "1.6.0"]]
  :profiles {:test {:resource-paths ["test-resources"]
                    :dependencies [[ch.qos.logback/logback-classic "1.1.8"]
                                   [ch.qos.logback/logback-core "1.1.8"]
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
  :test-selectors {:default (complement :integration)
                   :integration :integration}
  :repl-options {:init-ns konserve-ddb-s3.repl})
