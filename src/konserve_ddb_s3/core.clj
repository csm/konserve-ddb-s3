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
            [cognitect.aws.client.api :as aws-client]
            [clojure.tools.logging :as log]
            [superv.async :as sv]
            [clojure.string :as string])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream Closeable DataOutputStream DataInputStream]
           [java.util Base64 UUID]
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

(defn- encode-key
  "Encodes a key to URL-safe Base64."
  [prefix key]
  (.encodeToString (Base64/getUrlEncoder) (.getBytes (pr-str [prefix key]) "UTF-8")))

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

(defrecord DDB+S3Store [ddb-client s3-client table-name bucket-name dynamo-prefix s3-prefix serializer read-handlers write-handlers consistent-key locks clock]
  kp/PEDNAsyncKeyValueStore
  (-exists? [_ key]
    (let [begin (.instant clock)]
      (sv/go-try sv/S
        (if (consistent-key key)
          (loop [backoff 100]
            (let [begin (.instant clock)
                  ek (encode-key dynamo-prefix key)
                  response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                             :request {:TableName       table-name
                                                                       :Key             {"id" {:S ek}}
                                                                       :AttributesToGet ["val"]
                                                                       :ConsistentRead  true}}))]
              (log/debug {:task :ddb-get-item :phase :end :key ek :ms (ms clock begin)})
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
          (let [ek (encode-key s3-prefix key)
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
                            ek (encode-key dynamo-prefix k)
                            response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                                       :request {:TableName       table-name
                                                                                 :Key             {"id" {:S ek}}
                                                                                 :AttributesToGet ["val"]
                                                                                 :ConsistentRead  true}}))]
                        (log/debug {:task :ddb-get-item :phase :end :key ek :ms (ms clock begin)})
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
                    (let [s3k (encode-key s3-prefix k)
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
    (sv/go-try sv/S
      (let [[k & ks] key-vec]
        (if (consistent-key k)
          (loop [backoff 100]
            (let [begin (.instant clock)
                  ek (encode-key dynamo-prefix k)
                  response (async/<! (aws/invoke ddb-client {:op :GetItem
                                                             :request {:TableName       table-name
                                                                       :Key             {"id" {:S ek}}
                                                                       :AttributesToGet ["val" "rev"]
                                                                       :ConsistentRead  true}}))]
              (log/debug {:task :ddb-get-item :phase :end :key :ek :ms (ms clock begin)})
              (cond (throttle? response)
                    (do
                      (log/warn {:task :ddb-get-item :phase :throttled :backoff backoff})
                      (async/<! (async/timeout backoff))
                      (recur (min 60000 (* backoff 2))))

                    (anomaly? response)
                    (ex-info "failed to read DynamoDB" {:error response})

                    :else
                    (let [value (some->> response :val :B (kp/-deserialize serializer read-handlers))
                          rev (some-> response :rev :N (Long/parseLong))
                          new-value (if (empty? ks) (up-fn value) (update-in value ks up-fn))
                          new-rev (when rev (unchecked-inc rev))
                          begin (.instant clock)
                          encoded-value (let [out (ByteArrayOutputStream.)]
                                          (kp/-serialize serializer out write-handlers new-value)
                                          (.toByteArray out))
                          update-response (if (nil? rev)
                                            (let [r (async/<! (aws/invoke ddb-client {:op :PutItem
                                                                                      :request {:TableName table-name
                                                                                                :Item {"id"  {:S ek}
                                                                                                       "val" {:B encoded-value}
                                                                                                       "rev" {:N "0"}}
                                                                                                :ConditionExpression "attribute_not_exists(#id)"
                                                                                                :ExpressionAttributeNames {"#id" "id"}}}))]
                                              (log/debug {:task :ddb-put-item :phase :end :key ek :ms (ms clock begin)})
                                              r)
                                            (let [r (async/<! (aws/invoke ddb-client {:op :UpdateItem
                                                                                      :request {:TableName table-name
                                                                                                :Key {"id" {:S ek}}
                                                                                                :UpdateExpression "SET #val = :newval, #rev = :newrev"
                                                                                                :ConditionExpression "attribute_exists(#id) AND #rev = :oldrev"
                                                                                                :ExpressionAttributeNames {"#val" "val"
                                                                                                                           "#rev" "rev"
                                                                                                                           "#id" "id"}
                                                                                                :ExpressionAttributeValues {":newval" {:B encoded-value}
                                                                                                                            ":oldrev" {:N (str rev)}
                                                                                                                            ":newrev" {:N (str new-rev)}}}}))]
                                              (log/debug {:task :ddb-update-item :phase :end :key ek :ms (ms clock begin)})
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
          (let [ek (encode-key s3-prefix k)
                current-value (sv/<? sv/S (kp/-get-in this [k]))
                new-value (if (empty? ks)
                            (up-fn current-value)
                            (update-in current-value ks up-fn))
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
                ek (encode-key s3-prefix k)
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
          (let [ek (encode-key dynamo-prefix key)
                begin (.instant clock)
                response (async/<! (aws/invoke ddb-client {:op :DeleteItem
                                                           :request {:TableName table-name
                                                                     :Key       {"id" {:S ek}}}}))]
            (log/debug {:task :ddb-delete-item :phase :end :key ek :ms (ms clock begin)})
            (cond (throttle? response)
                  (do
                    (log/warn {:task :ddb-delete-item :phase :throttled :backoff backoff})
                    (async/<! (async/timeout backoff))
                    (recur (min 60000 (* backoff 2))))

                  (anomaly? response)
                  (ex-info "failed to delete from DynamoDB" {:error response})

                  :else nil)))
        (let [begin (.instant clock)
              ek (encode-key s3-prefix key)
              response (async/<! (aws/invoke s3-client {:op :DeleteObject
                                                        :request {:Bucket bucket-name
                                                                  :Key ek}}))]
          (log/debug {:task :s3-delete-object :phase :end :key ek :ms (ms clock begin)})
          (when (s/valid? ::anomalies/anomaly response)
            (ex-info "failed to delete S3 value" {:error response}))))))

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
                                                                 :AttributeDefinitions [{:AttributeName "id"
                                                                                         :AttributeType "S"}]
                                                                 :KeySchema [{:AttributeName "id"
                                                                              :KeyType "HASH"}]
                                                                 :ProvisionedThroughput {:ReadCapacityUnits read-throughput
                                                                                         :WriteCapacityUnits write-throughput}}}))
                     (if (and (= [{:AttributeName "id" :AttributeType "S"}] (-> table-exists :Table :AttributeDefinitions))
                              (= [{:AttributeName "id" :KeyType "HASH"}] (-> table-exists :Table :KeySchema)))
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
  [config]
  (async/go (ex-info "not yet implemented" {})))

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
                     (when-not (and (= [{:AttributeName "id" :AttributeType "S"}] (-> table-ok :Table :AttributeDefinitions))
                                    (= [{:AttributeName "id" :KeyType "HASH"}] (-> table-ok :Table :KeySchema)))
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table has invalid attribute definitions or key schema"}))]
      (if (s/valid? ::anomalies/anomaly table-ok)
        (ex-info "invalid dynamodb table" {:error table-ok})
        (let [bucket-ok (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))]
          (if (s/valid? ::anomalies/anomaly bucket-ok)
            (ex-info "invalid S3 bucket" {:error bucket-ok})
            (->DDB+S3Store ddb-client s3-client table bucket database database serializer read-handlers write-handlers consistent-key (atom {}) (nano-clock))))))))