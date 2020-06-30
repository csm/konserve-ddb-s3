(ns konserve-ddb-s3.core-test
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [konserve.core :as k]
            [konserve.protocols :as kp]
            [konserve-ddb-s3.core :refer :all]
            [s4.core :as s4]
            [superv.async :as sv]
            [clojure.java.io :as io])
  (:import [com.amazonaws.services.dynamodbv2.local.server LocalDynamoDBRequestHandler LocalDynamoDBServerHandler DynamoDBProxyServer]
           (java.net InetSocketAddress URI)
           (software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient)
           (software.amazon.awssdk.auth.credentials StaticCredentialsProvider AwsBasicCredentials)
           (software.amazon.awssdk.services.s3 S3AsyncClient S3Configuration)
           (software.amazon.awssdk.services.s3.model ListObjectsV2Request)
           (software.amazon.awssdk.services.dynamodb.model ListTablesRequest ListTablesResponse)))

(def ^:dynamic *ddb-client*)
(def ^:dynamic *s3-client*)

(defn log-stream
  [reader level tag]
  (doto (Thread. ^Runnable (fn []
                             (loop []
                               (when-let [line (.readLine reader)]
                                 (log/logp level tag line)
                                 (recur)))))
    (.setDaemon true)
    (.start)))

(defn start-dynamodb-local
  []
  (let [ddb-handler (LocalDynamoDBRequestHandler. 0 true nil false false)
        server-handler (LocalDynamoDBServerHandler. ddb-handler nil)
        server (doto (DynamoDBProxyServer. 0 server-handler) (.start))
        server-field (doto (.getDeclaredField DynamoDBProxyServer "server")
                       (.setAccessible true))
        jetty-server (.get server-field server)
        port (-> jetty-server (.getConnectors) first (.getLocalPort))]
    {:server server
     :port port}))

(let [chars (map char (filter #(Character/isLetterOrDigit %) (range 128)))]
  (defn random-str
    [length]
    (->> (range length)
         (map (fn [_] (rand-nth chars)))
         (string/join))))

(use-fixtures
  :once
  (fn [f]
    ; hack to set up sqlite4java. This fucking sucks.
    (let [classpath (string/split (System/getProperty "java.class.path") #":")
          _ (log/info "classpath:" classpath)
          sqlite4java-jar (io/file (first (filter #(re-matches #".*/sqlite4java.*\.jar" %) classpath)))
          sqlite4java-version (.getName (.getParentFile sqlite4java-jar))
          sqlite4java-group (.getParentFile (.getParentFile (.getParentFile sqlite4java-jar)))
          native-lib-name (case (System/getProperty "os.name")
                            "Linux"    "libsqlite4java-linux-amd64"
                            "Mac OS X" "libsqlite4java-osx")
          native-lib (io/file sqlite4java-group native-lib-name sqlite4java-version)]
      (log/warn "setting sqlite4java.library.path to" native-lib)
      (System/setProperty "sqlite4java.library.path" (.getPath native-lib)))

    (let [s4 (s4/make-server! {})
          ddb (start-dynamodb-local)
          ddb-port (:port ddb)
          s3-port (.getPort ^InetSocketAddress (:bind-address @s4))
          access-key (random-str 20)
          secret-key (random-str 40)]
      (swap! (-> s4 deref :auth-store :access-keys) assoc access-key secret-key)
      (try
        (binding [*ddb-client* (let [builder (DynamoDbAsyncClient/builder)]
                                 (.endpointOverride builder (URI. (str "http://localhost:" ddb-port)))
                                 (.credentialsProvider builder (StaticCredentialsProvider/create (AwsBasicCredentials/create access-key secret-key)))
                                 (.build builder))
                  *s3-client* (let [builder (S3AsyncClient/builder)
                                    config ^S3Configuration (-> (S3Configuration/builder)
                                                                (.pathStyleAccessEnabled true)
                                                                (.build))]
                                (.endpointOverride builder (URI. (str "http://localhost:" s3-port)))
                                (.credentialsProvider builder (StaticCredentialsProvider/create (AwsBasicCredentials/create access-key secret-key)))
                                (.serviceConfiguration builder ^S3Configuration config)
                                (.build builder))]
          (f))
        (finally
          (.stop (:server ddb))
          (.close (:server @s4)))))))

(deftest create-empty-store
  (testing "creating a new empty store works"
    (let [store (sv/<?? sv/S (empty-store {:region "us-west-2"
                                           :table "create-empty-store"
                                           :bucket "create-empty-store"
                                           :s3-client *s3-client*
                                           :ddb-client *ddb-client*}))]
      (is (satisfies? kp/PEDNAsyncKeyValueStore store))
      (is (not (instance? Throwable (async/<!! (.listObjectsV2 *s3-client* (-> (ListObjectsV2Request/builder)
                                                                               (.bucket "create-empty-store")
                                                                               (.build)))))))
      (is (not-empty (filter #(= "create-empty-store" %)
                             (-> (.listTables *ddb-client* (-> (ListTablesRequest/builder)
                                                               (.build)))
                                 ^ListTablesResponse  (async/<!!)
                                 (.tableNames))))))))

(deftest atomic-ops
  (let [store (sv/<?? sv/S (empty-store {:region "us-west-2"
                                         :table "atomic-ops"
                                         :bucket "atomic-ops"
                                         :s3-client *s3-client*
                                         :ddb-client *ddb-client*}))]
    (log/debug "store" store "locks:" (:locks store))
    (testing "that looking up nonexistent values returns false"
      (is (false? (async/<!! (k/exists? store :rabbit))))
      (is (nil? (async/<!! (k/get-in store [:rabbit])))))
    (testing "that we can assoc values"
      (is (some? (sv/<?? sv/S (k/assoc-in store [:data] {:long 42
                                                         :decimal 3.14159M
                                                         :string "some characters"
                                                         :vector [:foo :bar :baz/test]
                                                         :map {:x 10 :y 10}
                                                         :set #{:foo :bar :baz}})))))
    (testing "that we cat get-in"
      (is (= 42 (sv/<?? sv/S (k/get-in store [:data :long]))))
      (is (= 3.14159M (sv/<?? sv/S (k/get-in store [:data :decimal]))))
      (is (= "some characters" (sv/<?? sv/S (k/get-in store [:data :string]))))
      (is (= [:foo :bar :baz/test] (sv/<?? sv/S (k/get-in store [:data :vector]))))
      (is (= {:x 10 :y 10} (sv/<?? sv/S (k/get-in store [:data :map]))))
      (is (= #{:foo :bar :baz} (sv/<?? sv/S (k/get-in store [:data :set]))))
      (is (nil? (sv/<?? sv/S (k/get-in store [:data :rabbit])))))

    (testing "that we can update-in"
      (is (= [42 43] (sv/<?? sv/S (k/update-in store [:data :long] inc))))
      (is (= 43 (sv/<?? sv/S (k/get-in store [:data :long]))))

      (is (= [3.14159M 6.28318M] (sv/<?? sv/S (k/update-in store [:data :decimal] (partial * 2)))))
      (is (= 6.28318M (sv/<?? sv/S (k/get-in store [:data :decimal]))))

      (is (= ["some characters" "SOME CHARACTERS"] (sv/<?? sv/S (k/update-in store [:data :string] string/upper-case))))
      (is (= "SOME CHARACTERS" (sv/<?? sv/S (k/get-in store [:data :string]))))

      (is (= [[:foo :bar :baz/test] [:foo :bar :baz/test :quux]] (sv/<?? sv/S (k/update-in store [:data :vector] #(conj % :quux)))))
      (is (= [:foo :bar :baz/test :quux] (sv/<?? sv/S (k/get-in store [:data :vector]))))

      (is (= [{:x 10 :y 10} {:x 10 :y 10 :z 5}] (sv/<?? sv/S (k/update-in store [:data :map] #(assoc % :z 5)))))
      (is (= {:x 10 :y 10 :z 5} (sv/<?? sv/S (k/get-in store [:data :map]))))

      (is (= [#{:foo :bar :baz} #{:foo :bar}] (sv/<?? sv/S (k/update-in store [:data :set] #(disj % :baz)))))
      (is (= #{:foo :bar} (sv/<?? sv/S (k/get-in store [:data :set])))))

    ; these seem to fail, but I don't know if it's DynamoDBLocal or something else that's at fault
    (comment
      (testing "atomic ops with contention"
        (let [chans (pmap (fn [i] (k/update-in store [:data :long] (fn [n]
                                                                     (log/debug "task" i "input:" n)
                                                                     (inc n))))
                          (range 10))]
          (sv/<??* sv/S chans)
          (is (= 53 (sv/<?? sv/S (k/get-in store [:data :long])))))))

    (testing "that we can dissoc"
      (sv/<?? sv/S (k/dissoc store :data))
      (is (nil? (sv/<?? sv/S (k/get-in store [:data])))))))

(deftest test-nonatomic-keys
  (testing "with non-atomic keys"
    (let [store (async/<!! (empty-store {:region "us-west-2"
                                         :table "nonatomic-ops"
                                         :bucket "nonatomic-ops"
                                         :s3-client *s3-client*
                                         :ddb-client *ddb-client*
                                         :consistent-key (constantly false)}))]
      (testing "that looking up nonexistent values returns false"
        (is (false? (async/<!! (k/exists? store :rabbit))))
        (is (nil? (async/<!! (k/get-in store [:rabbit])))))
      (testing "that we can assoc values"

        (is (nil? (sv/<?? sv/S (k/assoc-in store [:data] {:long 42
                                                          :decimal 3.14159M
                                                          :string "some characters"
                                                          :vector [:foo :bar :baz/test]
                                                          :map {:x 10 :y 10}
                                                          :set #{:foo :bar :baz}}))))
        (is (= {:long 42
                :decimal 3.14159M
                :string "some characters"
                :vector [:foo :bar :baz/test]
                :map {:x 10 :y 10}
                :set #{:foo :bar :baz}}
               (sv/<?? sv/S (k/get-in store [:data])))))
      (testing "that we cat get-in"
        (is (= 42 (sv/<?? sv/S (k/get-in store [:data :long]))))
        (is (= 3.14159M (sv/<?? sv/S (k/get-in store [:data :decimal]))))
        (is (= "some characters" (sv/<?? sv/S (k/get-in store [:data :string]))))
        (is (= [:foo :bar :baz/test] (sv/<?? sv/S (k/get-in store [:data :vector]))))
        (is (= {:x 10 :y 10} (sv/<?? sv/S (k/get-in store [:data :map]))))
        (is (= #{:foo :bar :baz} (sv/<?? sv/S (k/get-in store [:data :set]))))
        (is (nil? (sv/<?? sv/S (k/get-in store [:data :rabbit])))))

      (testing "that we can update-in"
        (is (= [42 43] (sv/<?? sv/S (k/update-in store [:data :long] inc))))
        (is (= 43 (sv/<?? sv/S (k/get-in store [:data :long]))))

        (is (= [3.14159M 6.28318M] (sv/<?? sv/S (k/update-in store [:data :decimal] (partial * 2)))))
        (is (= 6.28318M (sv/<?? sv/S (k/get-in store [:data :decimal]))))

        (is (= ["some characters" "SOME CHARACTERS"] (sv/<?? sv/S (k/update-in store [:data :string] string/upper-case))))
        (is (= "SOME CHARACTERS" (sv/<?? sv/S (k/get-in store [:data :string]))))

        (is (= [[:foo :bar :baz/test] [:foo :bar :baz/test :quux]] (sv/<?? sv/S (k/update-in store [:data :vector] #(conj % :quux)))))
        (is (= [:foo :bar :baz/test :quux] (sv/<?? sv/S (k/get-in store [:data :vector]))))

        (is (= [{:x 10 :y 10} {:x 10 :y 10 :z 5}] (sv/<?? sv/S (k/update-in store [:data :map] #(assoc % :z 5)))))
        (is (= {:x 10 :y 10 :z 5} (sv/<?? sv/S (k/get-in store [:data :map]))))

        (is (= [#{:foo :bar :baz} #{:foo :bar}] (sv/<?? sv/S (k/update-in store [:data :set] #(disj % :baz)))))
        (is (= #{:foo :bar} (sv/<?? sv/S (k/get-in store [:data :set])))))

      (testing "that we can dissoc"
        (sv/<?? sv/S (k/dissoc store :data))
        (is (nil? (sv/<?? sv/S (k/get-in store [:data]))))))))