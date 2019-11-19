(ns konserve-ddb-s3.core
  "Konserve store implementation using DynamoDB and S3.

  For keys that must be atomic, DynamoDB is used to just
  hold addresses into S3, so when you assoc/update a key
  :foo that adds a key :foo (mangled with the database name)
  to dynamodb with value {:address S3_ADDR}; the data itself
  is stored in s3 with (mangled) key S3_ADDR, which is just
  a UUID.

  DynamoDB is always updated using a integer rev field
  that increments on each change; concurrent changes are
  retried, if they don't match the current rev value."
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api.async :as aws]
            [konserve.protocols :as kp]
            [konserve.serializers :as ser]
            [konserve-ddb-s3.b64 :as b64]
            [cognitect.aws.client.api :as aws-client]
            [clojure.tools.logging :as log]
            [superv.async :as sv]
            [clojure.string :as string]
            [clojure.edn :as edn])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream Closeable DataOutputStream DataInputStream]
           [java.util Base64]
           [java.time Clock Duration]
           [net.jpountz.lz4 LZ4Factory]
           [com.google.common.io ByteStreams]))

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
  "Encodes a key to sortable URL-safe Base64."
  [key]
  (b64/b64-encode (.getBytes (pr-str key) "UTF-8")))

(defn decode-key
  "Decode a key from sortable URL-safe base64."
  [b]
  (edn/read-string (String. (b64/b64-decode b) "UTF-8")))

(defn encode-s3-key
  [prefix key]
  (str (encode-key prefix) \. (encode-key key)))

(defn decode-s3-key
  [k]
  (map decode-key (string/split k #"\.")))

(defn- nano-clock
  ([] (nano-clock (Clock/systemUTC)))
  ([clock]
   (let [initial-instant (.instant clock)
         initial-nanos (System/nanoTime)]
     (proxy [Clock] []
       (getZone [] (.getZone clock))
       (withZone [zone] (nano-clock (.withZone clock zone)))
       (instant [] (.plusNanos initial-instant (- (System/nanoTime) initial-nanos)))))))

(defn- ms
  [clock begin]
  (-> (Duration/between begin (.instant clock))
      (.toNanos)
      (double)
      (/ 1000000.0)))

(defn- ddb-keys
  "Return a channel that yields all DynamoDB keys."
  [ddb-client table-name dynamo-prefix start-key]
  (let [ch (async/chan)
        encoded-prefix (encode-key dynamo-prefix)]
    (async/go-loop [start-key start-key
                    backoff 100]
      (let [ex-start-key (if (some? start-key)
                           {"tag" {:S encoded-prefix}
                            "key" {:S (encode-key start-key)}}
                           {"tag" {:S encoded-prefix}
                            "key" {:S "-"}}) ; minimal value in base-64 encoding
            scan-results (async/<! (aws/invoke ddb-client {:op :Scan
                                                           :request {:TableName table-name
                                                                     :ExclusiveStartKey ex-start-key
                                                                     :ScanFilter {"tag" {:AttributeValueList [{:S encoded-prefix}]
                                                                                         :ComparisonOperator "EQ"}}
                                                                     :AttributesToGet ["key"]}}))]
        (cond (throttle? scan-results)
              (do
                (async/<! (async/timeout backoff))
                (recur start-key (min 60000 (* 2 backoff))))

              (anomaly? scan-results)
              (do
                (async/>! ch (ex-info "failed to scan dynamodb table" {:error scan-results}))
                (async/close! ch))

              :else
              (let [ks (->> (scan-results :Items)
                            (map (comp :S :key))
                            (map decode-key))
                    last-key (loop [ks ks
                                    last-key nil]
                               (if-let [k (first ks)]
                                 (when (async/>! ch k)
                                   (recur (rest ks) k))
                                 last-key))]
                (if (and (some? last-key)
                         (some? (:LastEvaluatedKey scan-results))
                         (= (-> scan-results :LastEvaluatedKey :tag :S) encoded-prefix))
                  (recur last-key 100)
                  (async/close! ch))))))
    ch))

(defn- s3-keys
  [s3-client bucket-name s3-prefix start-key]
  (let [ch (async/chan)
        encoded-prefix (str (encode-key s3-prefix) \.)]
    (async/go-loop [encoded-prefix (if (some? start-key)
                                     (encode-s3-key s3-prefix start-key)
                                     encoded-prefix)
                    continuation-token nil]
      (log/debug "scanning s3 prefix:" encoded-prefix "continuation token:" continuation-token)
      (let [list-result (async/<! (aws/invoke s3-client {:op :ListObjectsV2
                                                         :request {:Bucket bucket-name
                                                                   :Prefix encoded-prefix
                                                                   :ContinuationToken continuation-token}}))]
        (log/debug "list result:" (dissoc list-result :Contents))
        (if (anomaly? list-result)
          (do
            (async/>! ch (ex-info "failed to read s3" {:error list-result}))
            (async/close! ch))
          (let [ks (->> (:Contents list-result)
                        (map :Key)
                        (map decode-s3-key))
                [last-prefix last-key] (loop [ks ks
                                              last-key nil]
                                         (if-let [k (first ks)]
                                           (when (async/>! ch (second k))
                                             (log/debug "put" k "remaining:" (count (rest ks)))
                                             (recur (rest ks) k))
                                           last-key))]
            (log/debug "last-prefix:" last-prefix "last-key:" last-key)
            (if (and (some? last-key)
                     (:IsTruncated list-result)
                     (= s3-prefix last-prefix))
              (recur encoded-prefix (:NextContinuationToken list-result))
              (async/close! ch))))))
    ch))

(defrecord DDB+S3Store [ddb-client s3-client table-name bucket-name dynamo-prefix s3-prefix serializer read-handlers write-handlers consistent-key locks clock]
  kp/PEDNAsyncKeyValueStore
  (-exists? [_ key]
    (let [begin (.instant clock)]
      (sv/go-try sv/S
        (if (consistent-key key)
          (loop [backoff 100]
            (let [begin (.instant clock)
                  ddb-key {"tag" {:S (encode-key dynamo-prefix)}
                           "key" {:S (encode-key key)}}
                  response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                             :request {:TableName       table-name
                                                                       :Key             ddb-key
                                                                       :AttributesToGet ["key"]
                                                                       :ConsistentRead  true}}))]
              (log/debug {:task :ddb-get-item :phase :end :key ddb-key :ms (ms clock begin)})
              (cond
                (throttle? response)
                (do
                  (log/warn :task :ddb-get-item :phase :throttled :backoff backoff)
                  (async/<! (async/timeout backoff))
                  (recur (min 60000 (* backoff 2))))

                (anomaly? response)
                (ex-info "failed to read dynamodb" {:error response})

                :else
                (not (empty? response)))))
          (let [ek (encode-s3-key s3-prefix key)
                response (async/<! (aws/invoke s3-client {:op :HeadObject
                                                          :request {:Bucket bucket-name
                                                                    :Key ek}}))]
            (log/debug {:task :s3-head-object :phase :end :key ek :ms (ms clock begin)})
            (cond (not-found? response)
                  false

                  (anomaly? response)
                  (ex-info "failed to read s3" {:error response})

                  :else
                  true))))))

  (-get-in [_ key-vec]
    (sv/go-try sv/S
      (let [[k & ks] key-vec
            consistent? (consistent-key k)
            value (if consistent?
                    (loop [backoff 100]
                      (let [begin (.instant clock)
                            ddb-key {"tag" {:S (encode-key dynamo-prefix)}
                                     "key" {:S (encode-key k)}}
                            response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                                       :request {:TableName       table-name
                                                                                 :Key             ddb-key
                                                                                 :AttributesToGet ["val"]
                                                                                 :ConsistentRead  true}}))]
                        (log/debug {:task :ddb-get-item :phase :end :key ddb-key :ms (ms clock begin)})
                        (cond (empty? response) nil

                              (throttle? response)
                              (do
                                (log/warn {:task :ddb-get-item :phase :throttled :backoff backoff})
                                (async/<! (async/timeout backoff))
                                (recur (min 60000 (* backoff 2))))

                              (anomaly? response)
                              (ex-info "failed to read dynamodb" {:error response})

                              :else
                              (kp/-deserialize serializer read-handlers (-> response :Item :val :B)))))
                    (let [s3k (encode-s3-key s3-prefix k)
                          begin (.instant clock)
                          response (async/<! (aws/invoke s3-client {:op :GetObject
                                                                    :request {:Bucket bucket-name
                                                                              :Key    s3k}}))]
                        (log/debug {:task :s3-get-object :phase :end :key s3k :ms (ms clock begin)})
                        (cond (and (not consistent?)
                                   (s/valid? ::anomalies/anomaly response)
                                   (= ::anomalies/not-found (::anomalies/category response)))
                              nil

                              (s/valid? ::anomalies/anomaly response)
                              (ex-info "failed to read S3" {:error response})

                              :else
                              (kp/-deserialize serializer read-handlers (:Body response)))))]
        (if (instance? Throwable value)
          value
          (get-in value ks)))))

  (-update-in [this key-vec up-fn]
    (kp/-update-in this key-vec up-fn []))

  (-update-in [this key-vec up-fn args]
    (sv/go-try sv/S
      (let [[k & ks] key-vec]
        (if (consistent-key k)
          (loop [backoff 100]
            (let [begin (.instant clock)
                  ddb-key {"tag" {:S (encode-key dynamo-prefix)}
                           "key" {:S (encode-key k)}}
                  response (async/<! (aws/invoke ddb-client {:op :GetItem
                                                             :request {:TableName       table-name
                                                                       :Key             ddb-key
                                                                       :AttributesToGet ["val" "rev"]
                                                                       :ConsistentRead  true}}))]
              (log/debug {:task :ddb-get-item :phase :end :key ddb-key :ms (ms clock begin)})
              (cond (throttle? response)
                    (do
                      (log/warn {:task :ddb-get-item :phase :throttled :backoff backoff})
                      (async/<! (async/timeout backoff))
                      (recur (min 60000 (* backoff 2))))

                    (anomaly? response)
                    (ex-info "failed to read DynamoDB" {:error response})

                    :else
                    (let [value (some->> response :Item :val :B (kp/-deserialize serializer read-handlers))
                          rev (some-> response :Item :rev :N (Long/parseLong))
                          new-value (if (empty? ks) (apply up-fn value args)
                                                    (update-in value ks #(apply up-fn % args)))
                          new-rev (when rev (unchecked-inc rev))
                          begin (.instant clock)
                          encoded-value (let [out (ByteArrayOutputStream.)]
                                          (kp/-serialize serializer out write-handlers new-value)
                                          (.toByteArray out))
                          update-response (if (nil? rev)
                                            (let [r (async/<! (aws/invoke ddb-client {:op :PutItem
                                                                                      :request {:TableName table-name
                                                                                                :Item (assoc ddb-key
                                                                                                             "val" {:B encoded-value}
                                                                                                             "rev" {:N "0"})
                                                                                                :ConditionExpression "attribute_not_exists(#tag) AND attribute_not_exists(#key)"
                                                                                                :ExpressionAttributeNames {"#tag" "tag" "#key" "key"}}}))]
                                              (log/debug {:task :ddb-put-item :phase :end :key ddb-key :ms (ms clock begin)})
                                              r)
                                            (let [r (async/<! (aws/invoke ddb-client {:op :UpdateItem
                                                                                      :request {:TableName table-name
                                                                                                :Key ddb-key
                                                                                                :UpdateExpression "SET #val = :newval, #rev = :newrev"
                                                                                                :ConditionExpression "attribute_exists(#tag) AND attribute_exists(#key) AND #rev = :oldrev"
                                                                                                :ExpressionAttributeNames {"#val" "val"
                                                                                                                           "#rev" "rev"
                                                                                                                           "#tag" "tag"
                                                                                                                           "#key" "key"}
                                                                                                :ExpressionAttributeValues {":newval" {:B encoded-value}
                                                                                                                            ":oldrev" {:N (str rev)}
                                                                                                                            ":newrev" {:N (str new-rev)}}}}))]
                                              (log/debug {:task :ddb-update-item :phase :end :key ddb-key :ms (ms clock begin)})
                                              r))]
                      (cond (throttle? update-response)
                            (do
                              (log/warn {:task (if rev :ddb-update-item :ddb-put-item) :phase :throttled :backoff backoff})
                              (async/<! (async/timeout backoff))
                              (recur (min 60000 (* backoff 2))))

                            (condition-failed? update-response)
                            (do
                              (log/info {:task (if rev :ddb-update-item :ddb-put-item) :phase :condition-failed})
                              (recur 100))

                            (anomaly? update-response)
                            (ex-info "failed to update dynamodb" {:error update-response})

                            :else
                            [(get-in value ks) (get-in new-value ks)])))))
          (let [ek (encode-s3-key s3-prefix k)
                current-value (sv/<? sv/S (kp/-get-in this [k]))
                new-value (if (empty? ks)
                            (apply up-fn current-value args)
                            (update-in current-value ks #(apply up-fn % args)))
                encoded (let [out (ByteArrayOutputStream.)]
                          (kp/-serialize serializer out write-handlers new-value)
                          (.toByteArray out))
                begin (.instant clock)
                response (async/<! (aws/invoke s3-client {:op :PutObject
                                                          :request {:Bucket bucket-name
                                                                    :Key    ek
                                                                    :Body   encoded}}))]
            (log/debug {:task :s3-put-object :phase :end :key ek :ms (ms clock begin)})
            (if (s/valid? ::anomalies/anomaly response)
              (ex-info "failed to write S3 object" {:error response})
              [(get-in current-value ks) (get-in new-value ks)]))))))

  (-assoc-in [this key-vec val]
    (let [[k & ks] key-vec]
      (if (or (consistent-key k) (not-empty ks))
        (kp/-update-in this key-vec (constantly val))
        (sv/go-try
          sv/S
          (let [encoded (let [out (ByteArrayOutputStream.)]
                          (kp/-serialize serializer out write-handlers val)
                          (.toByteArray out))
                ek (encode-s3-key s3-prefix k)
                begin (.instant clock)
                response (async/<! (aws/invoke s3-client {:op :PutObject
                                                          :request {:Bucket bucket-name
                                                                    :Key    ek
                                                                    :Body   encoded}}))]
            (log/debug {:task :s3-put-object :phase :end :key ek :ms (ms clock begin)})
            (when (anomaly? response)
              (ex-info "failed to write to S3" {:error response})))))))

  (-dissoc [_ key]
    (async/go
      (if (consistent-key key)
        (loop [backoff 100]
          (let [ddb-key {"tag" {:S (encode-key dynamo-prefix)}
                         "key" {:S (encode-key key)}}
                begin (.instant clock)
                response (async/<! (aws/invoke ddb-client {:op :DeleteItem
                                                           :request {:TableName table-name
                                                                     :Key       ddb-key}}))]
            (log/debug {:task :ddb-delete-item :phase :end :key ddb-key :ms (ms clock begin)})
            (cond (throttle? response)
                  (do
                    (log/warn {:task :ddb-delete-item :phase :throttled :backoff backoff})
                    (async/<! (async/timeout backoff))
                    (recur (min 60000 (* backoff 2))))

                  (anomaly? response)
                  (ex-info "failed to delete from DynamoDB" {:error response})

                  :else nil)))
        (let [begin (.instant clock)
              ek (encode-s3-key s3-prefix key)
              response (async/<! (aws/invoke s3-client {:op :DeleteObject
                                                        :request {:Bucket bucket-name
                                                                  :Key ek}}))]
          (log/debug {:task :s3-delete-object :phase :end :key ek :ms (ms clock begin)})
          (when (s/valid? ::anomalies/anomaly response)
            (ex-info "failed to delete S3 value" {:error response}))))))

  kp/PKeyIterable
  (-keys [_]
    (async/merge [(ddb-keys ddb-client table-name dynamo-prefix nil)
                  (s3-keys s3-client bucket-name s3-prefix nil)]))

  Closeable
  (close [_]
    (aws-client/stop ddb-client)
    (aws-client/stop s3-client)))

; serialized format:
; version          -- 1 byte, default 0
; uncompressed-len -- 4 bytes, uncompressed length, uint32 big-endian
; compressed-bytes -- rest is LZ4 compressed bytes

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
  * database -- A database identifier. This can be used to store different
    database values on the same table and bucket. Default nil.
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
  [{:keys [region table bucket database serializer read-handlers write-handlers read-throughput write-throughput ddb-client s3-client consistent-key]
    :or   {serializer default-serializer
           read-handlers (atom {})
           write-handlers (atom {})
           read-throughput 5
           write-throughput 5
           consistent-key (constantly true)}}]
  (async/go
    (let [ddb-client (or ddb-client (aws-client/client {:api :dynamodb :region region}))
          s3-client (or s3-client (aws-client/client {:api :s3 :region region :http-client (-> ddb-client .-info :http-client)}))
          table-exists (async/<! (aws/invoke ddb-client {:op :DescribeTable
                                                         :request {:TableName table}}))
          table-ok (if (s/valid? ::anomalies/anomaly table-exists)
                     (async/<! (aws/invoke ddb-client {:op :CreateTable
                                                       :request {:TableName table
                                                                 :AttributeDefinitions [{:AttributeName "tag"
                                                                                         :AttributeType "S"}
                                                                                        {:AttributeName "key"
                                                                                         :AttributeType "S"}]
                                                                 :KeySchema [{:AttributeName "tag"
                                                                              :KeyType "HASH"}
                                                                             {:AttributeName "key"
                                                                              :KeyType "RANGE"}]
                                                                 :ProvisionedThroughput {:ReadCapacityUnits read-throughput
                                                                                         :WriteCapacityUnits write-throughput}}}))
                     (if (and (= #{{:AttributeName "tag" :AttributeType "S"}
                                   {:AttributeName "key" :AttributeType "S"}}
                                 (-> table-exists :Table :AttributeDefinitions set))
                              (= #{{:AttributeName "tag" :KeyType "HASH"}
                                   {:AttributeName "key" :KeyType "RANGE"}}
                                 (-> table-exists :Table :KeySchema set)))
                       :ok
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table exists but has different attribute definitions or key schema than expected"}))]
      (if (s/valid? ::anomalies/anomaly table-ok)
        (ex-info "failed to initialize dynamodb" {:error table-ok})
        (let [bucket-exists (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))
              bucket-ok (cond (and (s/valid? ::anomalies/anomaly bucket-exists)
                                   (= ::anomalies/not-found (::anomalies/category bucket-exists)))
                              (async/<! (aws/invoke s3-client {:op :CreateBucket
                                                               :request {:Bucket bucket
                                                                         :CreateBucketConfiguration {:LocationConstraint region}}}))

                              (s/valid? ::anomalies/anomaly bucket-exists)
                              bucket-exists

                              :else :ok)]
          (if (s/valid? ::anomalies/anomaly bucket-ok)
            (ex-info "failed to initialize S3 (your dynamodb table will not be deleted if it was created)" {:error bucket-ok})
            (->DDB+S3Store ddb-client s3-client table bucket database database serializer read-handlers write-handlers consistent-key (atom {}) (nano-clock))))))))

(defn delete-store
  [{:keys [region table bucket database serializer read-handlers write-handlers ddb-client s3-client consistent-key]
    :or   {serializer default-serializer
           read-handlers (atom {})
           write-handlers (atom {})
           consistent-key (constantly true)}}]
  (sv/go-try sv/S
    (let [ddb-client (or ddb-client (aws-client/client {:api :dynamodb :region region}))
          s3-client (or s3-client (aws-client/client {:api :s3 :region region :http-client (-> ddb-client .-info :http-client)}))]
      (loop [backoff 100
             start-key nil]
        (let [items (async/<! (aws/invoke ddb-client {:op :Scan
                                                      :request {:TableName table
                                                                :ScanFilter {"tag" {:AttributeValueList [{:S (encode-key database)}]
                                                                                    :ComparisonOperator "EQ"}}
                                                                :ExclusiveStartKey start-key
                                                                :AttributesToGet ["tag" "key"]}}))]
          (cond (throttle? items)
                (do
                  (log/warn :task ::delete-store :phase :throttled :backoff backoff)
                  (async/<! (async/timeout backoff))
                  (recur (min 60000 (* 2 backoff)) start-key))

                (anomaly? items)
                (throw (ex-info "failed to scan dynamodb" {:error items}))

                :else
                (do
                  (loop [backoff 100
                         items (:Items items)]
                    (when-let [item (first items)]
                      (let [delete (async/<! (aws/invoke ddb-client {:op :DeleteItem
                                                                     :request {:TableName table
                                                                               :Key {"tag" (:tag item)
                                                                                     "key" (:key item)}}}))]
                        (cond (throttle? delete)
                              (do
                                (log/warn :task ::delete-store :phase :throttled :backoff backoff)
                                (async/<! (async/timeout backoff))
                                (recur (min 60000 (* 2 backoff)) items))

                              (anomaly? delete)
                              (throw (ex-info "failed to delete dynamodb item" {:error delete}))

                              :else
                              (recur 100 (rest items))))))
                  (when-let [k (:LastEvaluatedKey items)]
                    (recur 100 k))))))
      (loop [continuation-token nil]
        (let [prefix (str (encode-key database) \.)
              list (async/<! (aws/invoke s3-client {:op :ListObjectsV2
                                                    :request {:Bucket bucket
                                                              :Prefix prefix
                                                              :ContinuationToken continuation-token}}))]
          (if (anomaly? list)
            (throw (ex-info "failed to list S3 objects" {:error list}))
            (do
              (loop [objects (:Contents list)]
                (when-let [object (first objects)]
                  (let [delete (async/<! (aws/invoke s3-client {:op :DeleteObject
                                                                :request {:Bucket bucket
                                                                          :Key (:Key object)}}))]
                    (when (anomaly? delete)
                      (throw (ex-info "failed to delete S3 object" {:error delete}))))
                  (recur (rest objects))))
              (when (:IsTruncated list)
                (recur (:NextContinuationToken list))))))))))

(defn connect-store
  "Connect to an existing store on DynamoDB and S3.

  Keys in the argument map include:

  * region -- The AWS region. Required.
  * table -- The DynamoDB table name. Required.
  * bucket -- The S3 bucket name. Required.
  * database -- A database identifier. This can be used to store different
    database values on the same table and bucket. Default nil.
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
          table-ok (if (s/valid? ::anomalies/anomaly table-ok)
                     table-ok
                     (when-not (and (= #{{:AttributeName "tag" :AttributeType "S"}
                                         {:AttributeName "key" :AttributeType "S"}}
                                       (-> table-ok :Table :AttributeDefinitions set))
                                    (= #{{:AttributeName "tag" :KeyType "HASH"}
                                         {:AttributeName "key" :KeyType "RANGE"}}
                                       (-> table-ok :Table :KeySchema set)))
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table has invalid attribute definitions or key schema"}))]
      (if (s/valid? ::anomalies/anomaly table-ok)
        (ex-info "invalid dynamodb table" {:error table-ok})
        (let [bucket-ok (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))]
          (if (s/valid? ::anomalies/anomaly bucket-ok)
            (ex-info "invalid S3 bucket" {:error bucket-ok})
            (->DDB+S3Store ddb-client s3-client table bucket database database serializer read-handlers write-handlers consistent-key (atom {}) (nano-clock))))))))