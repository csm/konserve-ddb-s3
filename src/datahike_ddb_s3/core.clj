(ns datahike-ddb-s3.core
  "A konserve implementation spanning DynamoDB and S3. Intended
  for use in datahike only -- makes many assumptions based on
  datahike internals."
  (:require [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws-client]
            [cognitect.aws.client.api.async :as aws]
            [konserve.protocols :as kp]
            [konserve.serializers :as ser]
            [superv.async :as sv]
            [clojure.tools.logging :as log])
  (:import [java.util Base64]
           [java.io PushbackReader InputStreamReader ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream Closeable]
           [com.google.common.io ByteStreams]
           [net.jpountz.lz4 LZ4Factory]))

(defn anomaly?
  [result]
  (s/valid? ::anomalies/anomaly result))

(defn throttle?
  [result]
  (and (anomaly? result)
       (some? (:__type result))
       (string/includes? (:__type result) "ProvisionedThroughputExceeded")))

(defn condition-failed?
  [result]
  (and (anomaly? result)
       (string? (:message result))
       (string/includes? (:message result) "The conditional request failed")))

(defn not-found?
  [result]
  (and (anomaly? result) (= ::anomalies/not-found (::anomalies/category result))))

(defn encode-key
  [k]
  (str "k." (.encodeToString (Base64/getUrlEncoder) (.getBytes (pr-str k)))))

(defn decode-key
  [k]
  (if-let [k (second (re-matches #"k\.(.*)" (name k)))]
    (let [b (.decode (Base64/getUrlDecoder) k)
          reader (PushbackReader. (InputStreamReader. (ByteArrayInputStream. b)))]
      (edn/read reader))
    (throw (ex-info "invalid key read from dynamodb" {:key k}))))

(defprotocol IIndirect)

(defrecord S3Address [address]
  IIndirect)

(defn- get-db-value
  [{:keys [ddb-client table-name serializer read-handlers] :as this} ks]
  (sv/go-try sv/S
    (let [[k & ks] ks
          ddb-request (as-> {:TableName table-name
                             :ConsistentRead true
                             :Key {"k1" {:S "db"}
                                   "k2" {:S "@ROOT"}}}
                            req
                            (if k
                              (assoc req :AttributesToGet ["rev" (encode-key k)])
                              req))
          ddb-results (loop [backoff 100]
                        (let [response (async/<! (aws/invoke ddb-client {:op :GetItem
                                                                         :request ddb-request}))]
                          (cond (throttle? response)
                                (do
                                  (async/<! (async/timeout backoff))
                                  (recur (min 60000 (* 2 backoff))))

                                (anomaly? response)
                                (throw (ex-info "failed to read DynamoDB" {:error response}))

                                :else
                                response)))
          db-meta (-> ddb-results
                      :Item
                      (dissoc :k1 :k2 :rev)
                      (->> (reduce-kv (fn [m k v]
                                        (assoc m (decode-key k)
                                                 (kp/-deserialize serializer read-handlers (:B v))))
                                      {})))
          db-value (loop [in db-meta
                          out {}]
                     (let [[e & es] in]
                       (if e
                         (let [k (key e)
                               v (val e)]
                           (if-let [s3-address (when (map? v) (::s3-address v))]
                             (let [value (sv/<? sv/S (kp/-get-in this [s3-address]))]
                               (recur es (assoc out k (kp/-deserialize serializer read-handlers (:Body value)))))
                             (recur es (assoc out k v))))
                         out)))]
      {:value (when-not (empty? ddb-results) (get-in db-value ks))
       :rev   (-> ddb-results :Item :rev :N)})))

(defrecord DynamoDB+S3Store [ddb-client s3-client table-name bucket-name serializer read-handlers write-handlers locks]
  kp/PEDNAsyncKeyValueStore
  (-exists? [_ key]
    (sv/go-try sv/S
       (cond (= :db key) ; store db root metadata, op buffers in DynamoDB
             (loop [backoff 100]
               (let [response (async/<! (aws/invoke ddb-client {:op :GetItem
                                                                :request {:TableName table-name
                                                                          :Key {"k1" {:S "db"}
                                                                                "k2" {:S "@ROOT"}}
                                                                          :AttributesToGet ["k1" "k2"]}}))]
                 (cond (throttle? response)
                       (do
                         (async/<! (async/timeout backoff))
                         (recur (min 60000 (* 2 backoff))))

                       (anomaly? response)
                       (ex-info "failed to read DynamoDB" {:error response})

                       :else (not (empty? response)))))

             (= :ops key)
             (loop [backoff 100]
               (let [response (async/<! (aws/invoke ddb-client {:op :Scan
                                                                :request {:TableName table-name
                                                                          :ExclusiveStartKey {"k1" {:S "ops"}
                                                                                              "ks" {:S "0"}}
                                                                          :ScanFilter {"k1" {:AttributeValueList [{:S ":ops"}]
                                                                                             :ComparisonOperator "EQ"}}
                                                                          :Limit 1}}))]
                 (cond (throttle? response)
                       (do
                         (async/<! (async/timeout backoff))
                         (recur (min 60000 (* 2 backoff))))

                       (anomaly? response)
                       (ex-info "failed to read DynamoDB" {:error response})

                       :else
                       (not (empty? (:Items response))))))

             :else
             (let [response (async/<! (aws/invoke s3-client {:op :HeadObject
                                                             :request {:Bucket bucket-name
                                                                       :Key (str key)}}))]
               (cond (not-found? response)
                     false

                     (anomaly? response)
                     (ex-info "failed to read S3" {:error response})

                     :else true)))))

  (-get-in [this ks]
    (log/debug :task ::kp/-get-in :ks (pr-str ks))
    (sv/go-try sv/S
       (let [[k & ks] ks]
         (cond (= :db k)
               (:value (sv/<? sv/S (get-db-value this ks)))

               (= :ops k)
               (if (empty? ks)
                 (let [scan-result (loop [backoff 100
                                          start-key {"k1" {:S "ops"}
                                                     "k2" {:S "0"}}
                                          results []]
                                     (let [result (async/<! (aws/invoke ddb-client {:op :Scan
                                                                                    :request {:TableName table-name
                                                                                              :ExclusiveStartKey start-key
                                                                                              :ScanFilter {"k1" {:AttributeValueList [{:S "ops"}]
                                                                                                                 :ComparisonOperator "EQ"}}}}))]
                                       (cond (throttle? result)
                                             (do
                                               (async/<! (async/timeout backoff))
                                               (recur (min 60000 (* 2 backoff)) start-key results))

                                             (anomaly? result)
                                             (throw (ex-info "failed to read DynamoDB" {:error result}))

                                             (some? (:LastEvaluatedKey result))
                                             (recur 100 (:LastEvaluatedKey result)
                                                    (into results (map (fn [item] [(-> item :k2 :S)
                                                                                   (kp/-deserialize serializer read-handlers (-> item :val :B))]))))

                                             :else
                                             (into {} results))))]
                   scan-result)
                 (let [[k & ks] ks
                       item (loop [backoff 100]
                              (let [result (async/<! (aws/invoke ddb-client {:op :GetItem
                                                                             :request {:TableName table-name
                                                                                       :Key {"k1" {:S "ops"}
                                                                                             "k2" {:S (str k)}}}}))]
                                (cond (throttle? result)
                                      (do
                                        (async/<! (async/timeout backoff))
                                        (recur (min 60000 (* 2 backoff))))

                                      (anomaly? result)
                                      (throw (ex-info "failed to read DynamoDB" {:error result}))

                                      :else
                                      (kp/-deserialize serializer read-handlers (-> result :Item :val :B)))))]
                   (get-in item ks)))

               :else
               (let [result (async/<! (aws/invoke s3-client {:op :GetObject
                                                             :request {:Bucket bucket-name
                                                                       :Key k}}))]
                 (cond (not-found? result)
                       nil

                       (anomaly? result)
                       (ex-info "failed to read S3" {:error result})

                       :else
                       (kp/-deserialize serializer read-handlers (:Body result))))))))

  ; todo it would be nice to be able to append ops to the ops buffer in dynamodb. But for now we just swap in the new list.
  (-update-in [this ks f]
    (log/debug :task ::kp/-update-in :ks (pr-str ks) :f f)
    (sv/go-try sv/S
      (let [[k1 k2 & ks] ks]
        (cond (= :db k1)
              (loop [backoff 100]
                (let [{:keys [value rev]} (sv/<? sv/S (get-db-value this (some-> k2 vector)))
                      new-value (if (nil? k2)
                                  (f value)
                                  (update-in value (into (some-> k2 vector) ks) f))
                      ; todo do we want to store large items in S3? it may not be worth the effort.
                      update-result (if (nil? rev)
                                      (let [item (reduce-kv
                                                   (fn [m k v]
                                                     (assoc m (encode-key k) {:B (let [out (ByteArrayOutputStream.)]
                                                                                   (kp/-serialize serializer out write-handlers v)
                                                                                   (.toByteArray out))}))
                                                   {"k1" {:S "db"}
                                                    "k2" {:S "@ROOT"}
                                                    "rev" {:N "0"}}
                                                   new-value)]
                                        (async/<! (aws/invoke ddb-client {:op :PutItem
                                                                          :request {:TableName table-name
                                                                                    :Item item
                                                                                    :ConditionExpression "attribute_not_exists(k1) AND attribute_not_exists(k2)"}})))
                                      (let [update-names (assoc (->> (map-indexed (fn [i k]
                                                                                    [(str "#KEY" i) (encode-key k)])
                                                                                  (keys new-value))
                                                                     (into {}))
                                                           "#rev" "rev")
                                            update-values (assoc (->> (map-indexed (fn [i k]
                                                                                     [(str ":VALUE" i)
                                                                                      {:B (let [out (ByteArrayOutputStream.)]
                                                                                            (kp/-serialize serializer out write-handlers (get new-value k))
                                                                                            (.toByteArray out))}])
                                                                                   (keys new-value))
                                                                      (into {}))
                                                            ":oldrev" {:N rev}
                                                            ":newrev" {:N (-> (Long/parseLong rev) (unchecked-inc) str)})
                                            update-expression (str "SET "
                                                                   (string/join ", " (concat (map (fn [i] (str "#KEY" i " = :VALUE" i))
                                                                                                  (range (count (keys new-value))))
                                                                                             ["#rev = :newrev"])))]
                                        (log/debug :update-names (pr-str update-names)
                                                   :update-values (pr-str update-values)
                                                   :update-expression (pr-str update-expression))
                                        (async/<! (aws/invoke ddb-client {:op :UpdateItem
                                                                          :request {:TableName table-name
                                                                                    :Key {"k1" {:S "db"}
                                                                                          "k2" {:S "@ROOT"}}
                                                                                    :UpdateExpression update-expression
                                                                                    :ExpressionAttributeNames update-names
                                                                                    :ExpressionAttributeValues update-values
                                                                                    :ConditionExpression "#rev = :oldrev"}}))))]
                  (cond (throttle? update-result)
                        (do
                          (async/<! (async/timeout backoff))
                          (recur (min 60000 (* 2 backoff))))

                        (condition-failed? update-result)
                        (recur 100)

                        (anomaly? update-result)
                        (ex-info "failed to update DynamoDB" {:error update-result})

                        :else
                        [(get-in value ks)
                         (get-in new-value ks)])))

              (= :ops k1)
              (if (nil? k2)
                (ex-info "must write a sub-key of :ops" {})
                (loop [backoff 100]
                  (let [result (async/<! (aws/invoke ddb-client {:op :GetItem
                                                                 :request {:TableName table-name
                                                                           :Key {"k1" {:S "ops"}
                                                                                 "k2" {:S (str k2)}}
                                                                           :ConsistentRead true}}))]
                    (cond (throttle? result)
                          (do
                            (async/<! (async/timeout backoff))
                            (recur (min 60000 (* 2 backoff))))

                          (anomaly? result)
                          (ex-info "failed to read DynamoDB" {:error result})

                          :else
                          (let [rev (-> result :Item :rev :N)
                                value (some->> result
                                               :Item
                                               :val
                                               :B
                                               (kp/-deserialize serializer read-handlers))
                                new-value (if (empty? ks)
                                            (f value)
                                            (update-in value ks f))
                                encoded-value (let [out (ByteArrayOutputStream.)]
                                                (kp/-serialize serializer out write-handlers new-value)
                                                (.toByteArray out))
                                update-result (if (nil? rev)
                                                (async/<! (aws/invoke ddb-client {:op :PutItem
                                                                                  :request {:TableName table-name
                                                                                            :Item {"k1" {:S "ops"}
                                                                                                   "k2" {:S (str k2)}
                                                                                                   "rev" {:N "0"}
                                                                                                   "val" {:B encoded-value}}
                                                                                            :ConditionExpression "attribute_not_exists(k1) AND attribute_not_exists(k2)"}}))
                                                (async/<! (aws/invoke ddb-client {:op :UpdateItem
                                                                                  :request {:TableName table-name
                                                                                            :Key {"k1" {:S "ops"}
                                                                                                  "k2" {:S (str k2)}}
                                                                                            :UpdateExpression "SET #val = :val, #rev = #rev + 1"
                                                                                            :ConditionExpression "#rev = :rev"
                                                                                            :ExpressionAttributeNames {"#val" "val"
                                                                                                                       "#rev" "rev"}
                                                                                            :ExpressionAttributeValues {":val" {:B encoded-value}
                                                                                                                        ":rev" {:N rev}}}})))]
                            (cond (throttle? update-result)
                                  (do
                                    (async/<! (async/timeout backoff))
                                    (recur (min 60000 (* 2 backoff))))

                                  (condition-failed? update-result)
                                  (recur 100)

                                  (anomaly? update-result)
                                  (ex-info "failed to update DynamoDB" {:error update-result})

                                  :else
                                  [(get-in value ks) (get-in new-value ks)]))))))

              :else
              (let [value (let [result (async/<! (aws/invoke s3-client {:op :GetObject
                                                                        :request {:Bucket bucket-name
                                                                                  :Key (str k1)}}))]
                            (cond (not-found? result) nil
                                  (anomaly? result) (throw (ex-info "failed to read S3" {:error result}))
                                  :else (kp/-deserialize serializer read-handlers (:Body result))))
                    new-value (if (nil? k2)
                                (f value)
                                (update-in value (into [k2] ks) f))
                    encoded-value (let [out (ByteArrayOutputStream.)]
                                    (kp/-serialize serializer out write-handlers new-value)
                                    (.toByteArray out))
                    put-result (async/<! (aws/invoke s3-client {:op :PutObject
                                                                :request {:Bucket bucket-name
                                                                          :Key (str k1)
                                                                          :Body encoded-value}}))]
                (cond (anomaly? put-result)
                      (ex-info "failed to write to S3" {:error put-result})

                      (nil? k2)
                      [value new-value]

                      :else
                      [(get-in value (into [k2] ks))
                       (get-in new-value (into [k2] ks))]))))))

  (-assoc-in [this ks v]
    (log/debug :task ::kp/-assoc-in :ks (pr-str ks) :v (pr-str v))
    (kp/-update-in this ks (constantly v)))

  (-dissoc [this k]
    (async/go (UnsupportedOperationException. "not implemented")))

  Closeable
  (close [_]
    (aws-client/stop ddb-client)
    (aws-client/stop s3-client)))

(defn lz4-serializer
  "Wrap a konserve.protocols/PStoreSerializer such that serialized values
  are compressed with LZ4.

  Optional keyword argument :factory a net.jpountz.lz4.LZ4Factory. Defaults
  to `(net.jpountz.lz4.LZ4Factory/fastestInstance)`."
  [serializer & {:keys [factory] :or {factory (LZ4Factory/fastestInstance)}}]
  (let [compressor (.fastCompressor factory)
        decompressor (.fastDecompressor factory)]
    (reify kp/PStoreSerializer
      (-serialize [_ output-stream write-handlers val]
        (let [out (ByteArrayOutputStream.)]
          (kp/-serialize serializer out write-handlers val)
          (let [data-output (DataOutputStream. output-stream)
                serialized (.toByteArray out)
                serial-length (alength serialized)]
            (.writeByte data-output 0)
            (.writeInt data-output serial-length)
            (.write data-output (.compress compressor serialized))
            (.flush data-output))))
      (-deserialize [_ read-handlers input-stream]
        (let [data-input (DataInputStream. input-stream)
              _version (let [version (.readByte data-input)]
                         (when-not (zero? version)
                           (throw (ex-info "invalid object version" {:version version}))))
              uncompressed-len (.readInt data-input)
              bytes (ByteStreams/toByteArray data-input)
              decompressed (.decompress decompressor ^"[B" bytes uncompressed-len)]
          (kp/-deserialize serializer read-handlers (ByteArrayInputStream. decompressed)))))))

(def default-serializer
  (let [factory (LZ4Factory/fastestInstance)
        fressian (ser/fressian-serializer)]
    (lz4-serializer fressian :factory factory)))

(defn empty-store
  "Create an empty store.

    Keys in the argument map include:

    * region -- The AWS region. Required.
    * table -- The DynamoDB table name. Required.
    * bucket -- The S3 bucket name. Required.
    * serializer -- The value serializer; defaults to [[default-serializer]].
    * read-handlers -- An atom containing custom fressian read handlers.
    * write-handlers -- An atom containing custom fressian write handlers.
    * read-throughput -- Read throughput for the new DynamoDB table, if one is created. Default 5.
    * write-throughput -- Write throughput for the new DynamoDB table, if one is created. Default 5.
    * ddb-client -- An explicit DynamoDB client to use. Helpful for testing.
    * s3-client -- An explicit S3 client to use. Helpful for testing.
    * consistent-key -- A function of one argument that returns a truthy value
      if that key should be stored consistently. See docs for [[connect-store]]
      for implications of this argument. Default accepts all keys."
  [{:keys [region table bucket serializer read-handlers write-handlers read-throughput write-throughput ddb-client s3-client consistent-key]
    :or   {serializer       default-serializer
           read-handlers    (atom {})
           write-handlers   (atom {})
           read-throughput  5
           write-throughput 5
           consistent-key   (constantly true)}}]
  (async/go
    (let [ddb-client (or ddb-client (aws-client/client {:api :dynamodb :region region}))
          s3-client (or s3-client (aws-client/client {:api :s3 :region region :http-client (-> ddb-client .-info :http-client)}))
          table-exists (async/<! (aws/invoke ddb-client {:op :DescribeTable
                                                         :request {:TableName table}}))
          table-ok (if (s/valid? ::anomalies/anomaly table-exists)
                     (async/<! (aws/invoke ddb-client {:op :CreateTable
                                                       :request {:TableName table
                                                                 :AttributeDefinitions [{:AttributeName "k1" :AttributeType "S"}
                                                                                        {:AttributeName "k2" :AttributeType "S"}]
                                                                 :KeySchema [{:AttributeName "k1" :KeyType "HASH"}
                                                                             {:AttributeName "k2" :KeyType "RANGE"}]
                                                                 :ProvisionedThroughput {:ReadCapacityUnits read-throughput
                                                                                         :WriteCapacityUnits write-throughput}}}))
                     (if (and (= #{{:AttributeName "k1" :AttributeType "S"}
                                   {:AttributeName "k2" :AttributeType "S"}}
                                 (-> table-exists :Table :AttributeDefinitions set))
                              (= #{{:AttributeName "k1" :KeyType "HASH"}
                                   {:AttributeName "k2" :KeyType "RANGE"}}
                                 (-> table-exists :Table :KeySchema set)))
                       :ok
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table exists but has different attribute definitions or key schema than expected"}))]
      (if (s/valid? ::anomalies/anomaly table-ok)
        (ex-info "failed to initialize dynamodb" {:error table-ok})
        (let [bucket-exists (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))
              bucket-ok (cond (not-found? bucket-exists)
                              (async/<! (aws/invoke s3-client {:op :CreateBucket
                                                               :request {:Bucket bucket
                                                                         :CreateBucketConfiguration {:LocationConstraint region}}}))

                              (anomaly? bucket-exists)
                              bucket-exists

                              :else :ok)]
          (if (anomaly? bucket-ok)
            (ex-info "failed to initialize S3 (your dynamodb table will not be deleted if it was created)" {:error bucket-ok})
            (->DynamoDB+S3Store ddb-client s3-client table bucket serializer read-handlers write-handlers (atom {}))))))))

(defn delete-store
  [config]
  (async/go (ex-info "not yet implemented" {})))

(defn connect-store
  "Connect to an existing store on DynamoDB and S3.

  Keys in the argument map include:

  * region -- The AWS region. Required.
  * table -- The DynamoDB table name. Required.
  * bucket -- The S3 bucket name. Required.
  * serializer -- The value serializer; defaults to [[default-serializer]].
  * read-handlers -- An atom containing custom fressian read handlers.
  * write-handlers -- An atom containing custom fressian write handlers.
  * ddb-client -- An explicit DynamoDB client to use. Helpful for testing.
  * s3-client -- An explicit S3 client to use. Helpful for testing.
  * consistent-key -- A function of one argument that returns a truthy value
    if that key should be stored consistently. Default accepts all keys as
    consistent.

  The consistent-key function is a way to instruct the store which top-level
  keys must be atomic, and thus are stored across DynamoDB (for atomicity)
  AND S3 (to store the data). If this function returns false for a given key,
  it is assumed it does not need atomicity, and thus is only stored in S3.
  This is helpful for when you have some keys that must be atomic, and others
  that do not (for example, if a key is only ever written once, and then never
  updated)."
  [{:keys [region table bucket database serializer read-handlers write-handlers ddb-client s3-client consistent-key]
    :or   {serializer default-serializer
           read-handlers (atom {})
           write-handlers (atom {})
           consistent-key (constantly true)}}]
  (async/go
    (let [ddb-client (or ddb-client (aws-client/client {:api :dynamodb :region region}))
          s3-client (or s3-client (aws-client/client {:api :s3 :region region :http-client (-> ddb-client .-info :http-client)}))
          table-ok (async/<! (aws/invoke ddb-client {:op :DescribeTable :request {:TableName table}}))
          table-ok (if (anomaly? table-ok)
                     table-ok
                     (when-not (and (= #{{:AttributeName "k1" :AttributeType "S"}
                                         {:AttributeName "k2" :AttributeType "S"}}
                                       (-> table-ok :Table :AttributeDefinitions set))
                                    (= #{{:AttributeName "k1" :KeyType "HASH"}
                                         {:AttributeName "k2" :KeyType "RANGE"}}
                                       (-> table-ok :Table :KeySchema set)))
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table has invalid attribute definitions or key schema"}))]
      (if (anomaly? table-ok)
        (ex-info "invalid dynamodb table" {:error table-ok})
        (let [bucket-ok (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))]
          (if (s/valid? ::anomalies/anomaly bucket-ok)
            (ex-info "invalid S3 bucket" {:error bucket-ok})
            (->DynamoDB+S3Store ddb-client s3-client table bucket serializer read-handlers write-handlers (atom {}))))))))

(comment
  (aws/invoke ddb-client {:op :CreateTable
                          :request {:TableName "csm-datahike-test"
                                    :AttributeDefinitions [{:AttributeName "k1" :AttributeType "S"}
                                                           {:AttributeName "k2" :AttributeType "S"}]
                                    :KeySchema [{:AttributeName "k1" :KeyType "HASH"}
                                                {:AttributeName "k2" :KeyType "RANGE"}]
                                    :ProvisionedThroughput {:ReadCapacityUnits 1
                                                            :WriteCapacityUnits 1}}}))
