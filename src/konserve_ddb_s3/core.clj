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
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [konserve.protocols :as kp]
            [konserve.serializers :as ser]
            konserve-ddb-s3.async
            [konserve-ddb-s3.b64 :as b64]
            [superv.async :as sv])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream Closeable DataOutputStream DataInputStream]
           [java.util Base64 Collection]
           [java.time Clock Duration]
           [net.jpountz.lz4 LZ4Factory]
           [com.google.common.io ByteStreams]
           (software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (java.util.concurrent CompletableFuture)
           (java.util.function Function)
           (software.amazon.awssdk.services.dynamodb.model ScanRequest AttributeValue Condition ComparisonOperator ScanResponse GetItemRequest GetItemResponse ConditionalCheckFailedException PutItemRequest UpdateItemRequest DeleteItemRequest DescribeTableRequest AttributeDefinition ScalarAttributeType ResourceNotFoundException CreateTableRequest KeySchemaElement KeyType ProvisionedThroughput DescribeTableResponse BatchWriteItemRequest WriteRequest DeleteRequest TableStatus)
           (software.amazon.awssdk.services.s3.model ListObjectsV2Request ListObjectsV2Response S3Object HeadObjectRequest NoSuchKeyException GetObjectRequest PutObjectRequest DeleteObjectRequest HeadBucketRequest NoSuchBucketException CreateBucketRequest CreateBucketConfiguration BucketLocationConstraint)
           (software.amazon.awssdk.core.async AsyncResponseTransformer AsyncRequestBody)
           (software.amazon.awssdk.core ResponseBytes SdkBytes)
           (software.amazon.awssdk.regions Region)))

(defn condition-failed?
  [result]
  (or (instance? ConditionalCheckFailedException result)
      (instance? ConditionalCheckFailedException (.getCause result))))

(defn not-found?
  [result]
  (or (instance? NoSuchKeyException result)
      (instance? NoSuchKeyException (.getCause result))))

(defn no-such-bucket?
  [result]
  (or (instance? NoSuchBucketException result)
      (instance? NoSuchBucketException (.getCause result))))

(defn table-not-found?
  [result]
  (or (instance? ResourceNotFoundException result)
      (instance? ResourceNotFoundException (.getCause result))))

(defn encode-key
  "Encodes a key to sortable URL-safe Base64."
  [key]
  (b64/b64-encode (.getBytes (pr-str key) "UTF-8")))

(defn decode-key
  "Decode a key from sortable URL-safe base64."
  [b]
  (edn/read-string (String. ^"[B" (b64/b64-decode b) "UTF-8")))

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

(definline S [v] `(-> (AttributeValue/builder) (.s ~v) (.build)))
(definline B [v] `(-> (AttributeValue/builder) (.b (SdkBytes/fromByteArray ~v)) (.build)))
(definline N [v] `(-> (AttributeValue/builder) (.n (str ~v)) (.build)))

(comment
  (defn- ddb-keys
    "Return a channel that yields all DynamoDB keys."
    [^DynamoDbAsyncClient ddb-client table-name dynamo-prefix start-key]
    (let [ch (async/chan)
          encoded-prefix (encode-key dynamo-prefix)]
      (async/go-loop [start-key start-key]
        (let [ex-start-key (if (some? start-key)
                             {"tag" (S encoded-prefix)
                              "key" (S (encode-key start-key))}
                             {"tag" (S encoded-prefix)
                              "key" (S "-")}) ; minimal value in base-64 encoding
              scan-results (async/<!
                             (.scan ddb-client ^ScanRequest (-> (ScanRequest/builder)
                                                                (.tableName table-name)
                                                                (.exclusiveStartKey ex-start-key)
                                                                (.scanFilter {"tag" (-> (Condition/builder)
                                                                                        (.attributeValueList [(S encoded-prefix)])
                                                                                        (.comparisonOperator ComparisonOperator/EQ))})
                                                                (.attributesToGet ["key"])
                                                                (.build))))]
          (cond (instance? Throwable scan-results)
                (do
                  (async/>! ch scan-results)
                  (async/close! ch))

                :else
                (let [ks (->> ^ScanResponse
                              (.items)
                              (map #(.s ^AttributeValue (get % "key")))
                              (map decode-key))
                      last-key (loop [ks ks
                                      last-key nil]
                                 (if-let [k (first ks)]
                                   (when (async/>! ch k)
                                     (recur (rest ks) k))
                                   last-key))]
                  (if (and (some? last-key)
                           (some? (.lastEvaluatedKey scan-results))
                           (= (some-> scan-results ^AttributeValue (get "tag") (.s)) encoded-prefix))
                    (recur last-key)
                    (async/close! ch))))))
      ch)))

(comment
  (defn- s3-keys
    [^S3AsyncClient s3-client bucket-name s3-prefix start-key]
    (let [ch (async/chan)
          encoded-prefix (str (encode-key s3-prefix) \.)]
      (async/go-loop [encoded-prefix (if (some? start-key)
                                       (encode-s3-key s3-prefix start-key)
                                       encoded-prefix)
                      continuation-token nil]
        (log/debug "scanning s3 prefix:" encoded-prefix "continuation token:" continuation-token)
        (let [list-result (async/<!
                            (.listObjectsV2 s3-client ^ListObjectsV2Request (-> (ListObjectsV2Request/builder)
                                                                                (.bucket bucket-name)
                                                                                (.prefix encoded-prefix)
                                                                                (.continuationToken continuation-token)
                                                                                (.build))))]
          (log/debug "list result:" (dissoc list-result :Contents))
          (if (instance? Throwable list-result)
            (do
              (async/>! ch list-result)
              (async/close! ch))
            (let [ks (map #(decode-s3-key (.key ^S3Object %))
                          (.contents ^ListObjectsV2Response list-result))
                  [last-prefix last-key] (loop [ks ks
                                                last-key nil]
                                           (if-let [k (first ks)]
                                             (when (async/>! ch (second k))
                                               (log/debug "put" k "remaining:" (count (rest ks)))
                                               (recur (rest ks) k))
                                             last-key))]
              (log/debug "last-prefix:" last-prefix "last-key:" last-key)
              (if (and (some? last-key)
                       (.isTruncated list-result)
                       (= s3-prefix last-prefix))
                (recur encoded-prefix (.nextContinuationToken list-result))
                (async/close! ch))))))
      ch)))

(defrecord DDB+S3Store [^DynamoDbAsyncClient ddb-client ^S3AsyncClient s3-client table-name bucket-name dynamo-prefix s3-prefix serializer read-handlers write-handlers consistent-key locks clock]
  kp/PEDNAsyncKeyValueStore
  (-exists? [_ key]
    (let [begin (.instant clock)]
      (sv/go-try sv/S
        (if (consistent-key key)
          (let [begin (.instant clock)
                ddb-key {"tag" (S (encode-key dynamo-prefix))
                         "key" (S (encode-key key))}
                response (async/<!
                           (.getItem ddb-client (-> (GetItemRequest/builder)
                                                    (.tableName table-name)
                                                    (.key ddb-key)
                                                    (.attributesToGet ["key"])
                                                    (.consistentRead true)
                                                    (.build))))]
            (log/debug {:task :ddb-get-item :phase :end :key ddb-key :ms (ms clock begin)})
            (cond
              (instance? Throwable response)
              response

              :else
              (not (empty? (.item ^GetItemResponse response)))))
          (let [ek (encode-s3-key s3-prefix key)
                response (async/<!
                           (.headObject s3-client (-> (HeadObjectRequest/builder)
                                                      (.bucket bucket-name)
                                                      (.key ek)
                                                      (.build))))]
            (log/debug {:task :s3-head-object :phase :end :key ek :ms (ms clock begin)})
            (if (instance? Throwable response)
              (if (not-found? response)
                false
                response)
              true))))))

  (-get-in [_ key-vec]
    (sv/go-try sv/S
      (let [[k & ks] key-vec
            consistent? (consistent-key k)
            value (if consistent?
                    (let [begin (.instant clock)
                          ddb-key {"tag" (S (encode-key dynamo-prefix))
                                   "key" (S (encode-key k))}
                          response (async/<!
                                     (.getItem ddb-client (-> (GetItemRequest/builder)
                                                              (.tableName table-name)
                                                              (.key ddb-key)
                                                              (.attributesToGet ["val"])
                                                              (.consistentRead true)
                                                              (.build))))]
                      (log/debug {:task :ddb-get-item :phase :end :key ddb-key :ms (ms clock begin)})
                      (if (instance? Throwable response)
                        response
                        (when-let [b (some-> response (.item) ^AttributeValue (get "val") (.b) (.asByteArray))]
                          (kp/-deserialize serializer read-handlers (ByteArrayInputStream. b)))))
                    (let [s3k (encode-s3-key s3-prefix k)
                          begin (.instant clock)
                          response (async/<!
                                     (.getObject s3-client ^GetObjectRequest (-> (GetObjectRequest/builder)
                                                                                 (.bucket bucket-name)
                                                                                 (.key s3k)
                                                                                 (.build))
                                                 (AsyncResponseTransformer/toBytes)))]
                        (log/debug {:task :s3-get-object :phase :end :key s3k :ms (ms clock begin)})
                        (if (instance? Throwable response)
                          (if (not-found? response)
                            nil
                            response)
                          (kp/-deserialize serializer read-handlers (ByteArrayInputStream. (.asByteArray ^ResponseBytes response))))))]
        (if (instance? Throwable value)
          value
          (get-in value ks)))))

  (-update-in [this key-vec up-fn]
    (kp/-update-in this key-vec up-fn []))

  (-update-in [this key-vec up-fn args]
    (sv/go-try sv/S
      (let [[k & ks] key-vec]
        (if (consistent-key k)
          (loop []
            (let [begin (.instant clock)
                  ddb-key {"tag" (S (encode-key dynamo-prefix))
                           "key" (S (encode-key k))}
                  response (async/<!
                             (.getItem ddb-client (-> (GetItemRequest/builder)
                                                      (.tableName table-name)
                                                      (.key ddb-key)
                                                      (.attributesToGet ["val" "rev"])
                                                      (.consistentRead true)
                                                      (.build))))]
              (log/debug {:task :ddb-get-item :phase :end :key ddb-key :ms (ms clock begin)})
              (if (instance? Throwable response)
                response
                (let [value (some-> response
                                    (.item)
                                    ^AttributeValue (get "val")
                                    (.b)
                                    (.asByteArray)
                                    (ByteArrayInputStream.)
                                    (->> (kp/-deserialize serializer read-handlers)))
                      rev (some-> response (.item) ^AttributeValue (get "rev") (.n) (Long/parseLong))
                      new-value (if (empty? ks) (apply up-fn value args)
                                                (update-in value ks #(apply up-fn % args)))
                      new-rev (when rev (unchecked-inc rev))
                      begin (.instant clock)
                      encoded-value (let [out (ByteArrayOutputStream.)]
                                      (kp/-serialize serializer out write-handlers new-value)
                                      (.toByteArray out))
                      update-response (if (nil? rev)
                                        (let [r (async/<! (.putItem ddb-client (-> (PutItemRequest/builder)
                                                                                   (.tableName table-name)
                                                                                   (.item (assoc ddb-key
                                                                                            "val" (B encoded-value)
                                                                                            "rev" (N 0)))
                                                                                   (.conditionExpression "attribute_not_exists(#tag) AND attribute_not_exists(#key)")
                                                                                   (.expressionAttributeNames {"#tag" "tag"
                                                                                                               "#key" "key"})
                                                                                   (.build))))]
                                          (log/debug {:task :ddb-put-item :phase :end :key ddb-key :ms (ms clock begin)})
                                          r)
                                        (let [r (async/<! (.updateItem ddb-client (-> (UpdateItemRequest/builder)
                                                                                      (.tableName table-name)
                                                                                      (.key ddb-key)
                                                                                      (.updateExpression "SET #val = :newval, #rev = :newrev")
                                                                                      (.conditionExpression "attribute_exists(#tag) AND attribute_exists(#key) AND #rev = :oldrev")
                                                                                      (.expressionAttributeNames {"#val" "val"
                                                                                                                  "#rev" "rev"
                                                                                                                  "#tag" "tag"
                                                                                                                  "#key" "key"})
                                                                                      (.expressionAttributeValues {":newval" (B encoded-value)
                                                                                                                   ":oldrev" (N rev)
                                                                                                                   ":newrev" (N new-rev)})
                                                                                      (.build))))]
                                          (log/debug {:task :ddb-update-item :phase :end :key ddb-key :ms (ms clock begin)})
                                          r))]
                  (if (instance? Throwable update-response)
                    (if (condition-failed? update-response)
                      (do
                        (log/info {:task (if rev :ddb-update-item :ddb-put-item) :phase :condition-failed})
                        (recur))
                      update-response)
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
                response (async/<!
                           (.putObject s3-client (-> (PutObjectRequest/builder)
                                                     (.bucket bucket-name)
                                                     (.key ek)
                                                     (.build))
                                       (AsyncRequestBody/fromBytes encoded)))]
            (log/debug {:task :s3-put-object :phase :end :key ek :ms (ms clock begin)})
            (if (instance? Throwable response)
              response
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
                response (async/<!
                           (.putObject s3-client (-> (PutObjectRequest/builder)
                                                     (.bucket bucket-name)
                                                     (.key ek)
                                                     (.build))
                                       (AsyncRequestBody/fromBytes encoded)))]
            (log/debug {:task :s3-put-object :phase :end :key ek :ms (ms clock begin)})
            (if (instance? Throwable response)
              response
              val))))))

  (-dissoc [_ key]
    (async/go
      (if (consistent-key key)
        (let [ddb-key {"tag" (S (encode-key dynamo-prefix))
                       "key" (S (encode-key key))}
              begin (.instant clock)
              response (async/<!
                         (.deleteItem ddb-client (-> (DeleteItemRequest/builder)
                                                     (.tableName table-name)
                                                     (.key ddb-key)
                                                     (.build))))]
          (log/debug {:task :ddb-delete-item :phase :end :key ddb-key :ms (ms clock begin)})
          (when (instance? Throwable response)
            response))
        (let [begin (.instant clock)
              ek (encode-s3-key s3-prefix key)
              response (async/<!
                         (.deleteObject s3-client (-> (DeleteObjectRequest/builder)
                                                      (.bucket bucket-name)
                                                      (.key ek)
                                                      (.build))))]
          (log/debug {:task :s3-delete-object :phase :end :key ek :ms (ms clock begin)})
          (when (instance? Throwable response)
            response)))))

  ;kp/PKeyIterable
  ;(-keys [_]
  ;  (async/merge [(ddb-keys ddb-client table-name dynamo-prefix nil)
  ;                (s3-keys s3-client bucket-name s3-prefix nil))))))

  Closeable
  (close [_]
    (.close ddb-client)
    (.close s3-client)))

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


(def ^:private attrib-defs [(-> (AttributeDefinition/builder)
                                (.attributeName "tag")
                                (.attributeType ScalarAttributeType/S)
                                (.build))
                            (-> (AttributeDefinition/builder)
                                (.attributeName "key")
                                (.attributeType ScalarAttributeType/S)
                                (.build))])
(def ^:private key-schema [(-> (KeySchemaElement/builder)
                               (.attributeName "tag")
                               (.keyType KeyType/HASH)
                               (.build))
                           (-> (KeySchemaElement/builder)
                               (.attributeName "key")
                               (.keyType KeyType/RANGE)
                               (.build))])

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
           read-throughput 1
           write-throughput 1
           consistent-key (constantly true)}}]
  (async/go
    (let [ddb-client (or ddb-client (-> (DynamoDbAsyncClient/builder)
                                        (.region (Region/of region))
                                        (.build)))
          s3-client (or s3-client (-> (S3AsyncClient/builder)
                                      (.region (Region/of region))
                                      (.build)))
          table-exists (async/<! (.describeTable ddb-client (-> (DescribeTableRequest/builder)
                                                                (.tableName table)
                                                                (.build))))

          table-ok (if (instance? Throwable table-exists)
                     (if (table-not-found? table-exists)
                       (do (async/<! (.createTable ddb-client (-> (CreateTableRequest/builder)
                                                                (.tableName table)
                                                                (.attributeDefinitions ^Collection attrib-defs)
                                                                (.keySchema ^Collection key-schema)
                                                                (.provisionedThroughput ^ProvisionedThroughput (-> (ProvisionedThroughput/builder)
                                                                                                                   (.readCapacityUnits read-throughput)
                                                                                                                   (.writeCapacityUnits write-throughput)
                                                                                                                   (.build)))
                                                                (.build))))
                           (loop []
                             (let [table-info (async/<! (.describeTable ddb-client (-> (DescribeTableRequest/builder)
                                                                                       (.tableName table)
                                                                                       (.build))))]
                               (cond (instance? Throwable table-info)
                                     table-info

                                     (not= (-> table-info (.table) (.tableStatus)) TableStatus/ACTIVE)
                                     (do
                                       (log/debug "waiting for table" table "to become ready...")
                                       (Thread/sleep 1000)
                                       (recur))

                                     :else :ok))))
                       table-exists)
                     (if (and (= (set attrib-defs)
                                 (-> ^DescribeTableResponse table-exists (.table) (.attributeDefinitions) (set)))
                              (= (set key-schema)
                                 (-> ^DescribeTableResponse table-exists (.table) (.keySchema) (set))))
                       :ok
                       (ex-info "table exists but has an incompatible schema" {:table-name table})))]
      (if (instance? Throwable table-ok)
        table-ok
        (let [bucket-exists (async/<! (.headBucket s3-client (-> (HeadBucketRequest/builder)
                                                                 (.bucket bucket)
                                                                 (.build))))
              bucket-ok (cond (and (instance? Throwable bucket-exists)
                                   (no-such-bucket? bucket-exists))
                              (async/<! (.createBucket s3-client (-> (CreateBucketRequest/builder)
                                                                     (.bucket bucket)
                                                                     (.createBucketConfiguration ^CreateBucketConfiguration (-> (CreateBucketConfiguration/builder)
                                                                                                                                (.locationConstraint (BucketLocationConstraint/fromValue region))
                                                                                                                                (.build)))
                                                                     (.build))))

                              (instance? Throwable bucket-exists)
                              bucket-exists

                              :else :ok)]
          (if (instance? Throwable bucket-ok)
            bucket-ok
            (map->DDB+S3Store {:ddb-client ddb-client
                               :s3-client s3-client
                               :table-name table
                               :bucket-name bucket
                               :dynamo-prefix database
                               :s3-prefix database
                               :serializer serializer
                               :read-handlers read-handlers
                               :write-handlers write-handlers
                               :consistent-key consistent-key
                               :locks (atom {})
                               :clock (nano-clock)})))))))

(defn delete-store
  [{:keys [region table bucket database ddb-client s3-client]}]
  (sv/go-try sv/S
    (let [ddb-client (or ddb-client (-> (DynamoDbAsyncClient/builder)
                                        (.region (Region/of region))
                                        (.build)))
          s3-client (or s3-client (-> (S3AsyncClient/builder)
                                      (.region (Region/of region))
                                      (.build)))
          clock (nano-clock)]
      (loop [start-key nil]
        (log/debug :task :ddb-scan :phase :begin)
        (let [begin (.instant clock)
              items (async/<! (.scan ddb-client (-> (ScanRequest/builder)
                                                    (.tableName table)
                                                    (.scanFilter {"tag" (-> (Condition/builder)
                                                                            (.attributeValueList [(S (encode-key database))])
                                                                            (.comparisonOperator ComparisonOperator/EQ)
                                                                            (.build))})
                                                    (.attributesToGet ["tag" "key"])
                                                    (.build))))]
          (log/debug :task :ddb-scan :phase :end :ms (ms clock begin))
          (if (instance? Throwable items)
            (throw items)
            (do
              (loop [items (partition-all 25 (.items ^ScanResponse items))]
                (when-let [batch (first items)]
                  (log/debug :task :ddb-delete-items :count (count batch) :phase :begin)
                  (let [begin (.instant clock)
                        delete (async/<! (.batchWriteItem ddb-client (-> (BatchWriteItemRequest/builder)
                                                                         (.requestItems {table (map (fn [item]
                                                                                                      (-> (WriteRequest/builder)
                                                                                                          (.deleteRequest ^DeleteRequest (-> (DeleteRequest/builder)
                                                                                                                                             (.key item)
                                                                                                                                             (.build)))
                                                                                                          (.build)))
                                                                                                    batch)})
                                                                         (.build))))]
                    (log/debug :task :ddb-delete-item :phase :end :ms (ms clock begin))
                    (if (instance? Throwable delete)
                      (throw delete)
                      (recur (rest items))))))
              (when-let [k (not-empty (.lastEvaluatedKey items))]
                (recur k))))))
      (loop [continuation-token nil]
        (log/debug :task :s3-list-objects :phase :begin)
        (let [prefix (str (encode-key database) \.)
              begin (.instant clock)
              list (async/<! (.listObjectsV2 s3-client (-> (ListObjectsV2Request/builder)
                                                           (.bucket bucket)
                                                           (.prefix prefix)
                                                           (.continuationToken continuation-token)
                                                           (.build))))]
          (log/debug :task :s3-list-objects :phase :end :ms (ms clock begin))
          (if (instance? Throwable list)
            (throw list)
            (do
              (loop [objects (.contents list)]
                (when-let [object (first objects)]
                  (log/debug :task :s3-delete-object :key (:Key object) :phase :begin)
                  (let [begin (.instant clock)
                        delete (.deleteObject s3-client (-> (DeleteObjectRequest/builder)
                                                            (.bucket bucket)
                                                            (.key (.key ^S3Object object))
                                                            (.build)))]
                    (log/debug :task :s3-delete-object :phase :end :ms (ms clock begin))
                    (when (instance? Throwable delete)
                      (throw delete)))
                  (recur (rest objects))))
              (when (.isTruncated list)
                (recur (.nextContinuationToken list))))))))))

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
    (let [ddb-client (or ddb-client (-> (DynamoDbAsyncClient/builder)
                                        (.region (Region/of region))
                                        (.build)))
          s3-client (or s3-client (-> (S3AsyncClient/builder)
                                      (.region (Region/of region))
                                      (.build)))
          table-ok (async/<! (.describeTable ddb-client (-> (DescribeTableRequest/builder)
                                                            (.tableName table)
                                                            (.build))))
          table-ok (if (instance? Throwable table-ok)
                     table-ok
                     (when-not (and (= attrib-defs
                                       (-> table-ok (.table) (.attributeDefinitions)))
                                    (= key-schema
                                       (-> table-ok (.table) (.keySchema))))
                       (ex-info "table exists but has invalid schema" {:table-name table})))]
      (if (instance? Throwable table-ok)
        table-ok
        (let [bucket-ok (async/<! (.headBucket s3-client (-> (HeadBucketRequest/builder)
                                                             (.bucket bucket)
                                                             (.build))))]
          (if (instance? Throwable bucket-ok)
            bucket-ok
            (map->DDB+S3Store {:ddb-client ddb-client
                               :s3-client s3-client
                               :table-name table
                               :bucket-name bucket
                               :dynamo-prefix database
                               :s3-prefix database
                               :serializer serializer
                               :read-handlers read-handlers
                               :write-handlers write-handlers
                               :consistent-key consistent-key
                               :locks (atom {})
                               :clock (nano-clock)})))))))