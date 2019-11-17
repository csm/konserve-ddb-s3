(in-ns 'konserve-ddb-s3.repl)

(require '[cognitect.aws.client.api :as aws])

(def region)
(def ddb-table-name)
(def s3-bucket-name)

(def ddb-client (aws/client {:api :dynamodb :region region}))
(def s3-client (aws/client {:api :s3 :region region}))

(require '[clojure.edn :as edn])
(import '[java.util Base64])

(defn old-decode-key
  [s]
  (edn/read-string (String. (.decode (Base64/getUrlDecoder) s) "UTF-8")))

(require '[konserve-ddb-s3.core :refer [throttle? anomaly?] :as k] :reload)
(def encode-key #'konserve-ddb-s3.core/encode-key)

(defn rename-key
  [k]
  (let [[tag key] (old-decode-key k)]
    (encode-key tag key)))
(import '[com.google.common.io ByteStreams])

(loop [last-key nil
       backoff 100]
  (let [scan-result (aws/invoke ddb-client {:op :Scan
                                            :request {:TableName ddb-table-name
                                                      :ExclusiveStartKey last-key}})]
    (cond (throttle? scan-result)
          (do
            (println "backoff" backoff)
            (Thread/sleep backoff)
            (recur last-key (min 60000 (* 2 backoff))))

          (anomaly? scan-result)
          (throw (ex-info "failed to scan dynamodb" {:error scan-result}))

          :else
          (let [items (:Items scan-result)]
            (loop [items items
                   backoff 100]
              (when-let [item (first items)]
                (println "item key:" (:id item))
                (let [new-item (-> (update-in item [:id :S] rename-key)
                                   (update-in [:val :B] #(ByteStreams/toByteArray %)))
                      _ (println (pr-str (select-keys item [:id])) "->" (pr-str (select-keys new-item [:id])))
                      put-result (aws/invoke ddb-client {:op :PutItem
                                                         :request {:TableName ddb-table-name
                                                                   :Item new-item}})]
                  (cond
                    (throttle? put-result)
                    (do
                      (println "throttle backoff" backoff)
                      (Thread/sleep backoff)
                      (recur items (min 60000 (* 2 backoff))))

                    (anomaly? put-result)
                    (throw (ex-info "failed to put new key" {:error put-result :key (:id item) :new-key (:id new-item)}))

                    :else
                    (recur (rest items) 100)))))
            (loop [items items
                   backoff 100]
              (when-let [item (first items)]
                (let [delete-result (aws/invoke ddb-client {:op      :DeleteItem
                                                            :request {:TableName ddb-table-name
                                                                      :Key       (select-keys item [:id])}})]
                  (cond
                    (throttle? delete-result)
                    (do
                      (println "throttle backoff" backoff)
                      (Thread/sleep backoff)
                      (recur items (min 60000 (* 2 backoff))))

                    (anomaly? delete-result)
                    (throw (ex-info "failed to delete old key" {:error delete-result :key (:id item)}))

                    :else
                    (recur (rest items) 100)))))
            (when-let [last-key (:LastEvaluatedKey scan-result)]
              (recur last-key 100))))))

(loop [continuation-token nil]
  (let [list-result (aws/invoke s3-client {:op :ListObjectsV2
                                           :request {:Bucket s3-bucket-name
                                                     :Prefix "W"
                                                     :ContinuationToken continuation-token}})]
    (if (anomaly? list-result)
      (throw (ex-info "failed to list objects" {:error list-result}))
      (do
        (doseq [item (:Contents list-result)]
          (let [new-key (rename-key (:Key item))
                _ (println "copy" (:Key item) "->" new-key)
                copy-result (aws/invoke s3-client {:op :CopyObject
                                                   :request {:Bucket s3-bucket-name
                                                             :CopySource (str s3-bucket-name "/" (:Key item))
                                                             :Key new-key}})]
            (if (anomaly? copy-result)
              (throw (ex-info "failed to copy object" {:error copy-result
                                                       :key (:Key item)
                                                       :new-key new-key}))
              (let [_ (println "delete" (:Key item))
                    delete-result (aws/invoke s3-client {:op :DeleteObject
                                                         :request {:Bucket s3-bucket-name
                                                                   :Key (:Key item)}})]
                (when (anomaly? delete-result)
                  (throw (ex-info "failed to delete object" {:error delete-result
                                                             :key (:Key item)})))))))
        (when-let [t (:NextContinuationToken list-result)]
          (recur t))))))

(require '[clojure.string :as string])

(defn rename-s3-key
  [k]
  (let [[tag key] (k/decode-key k)]
    (str (k/encode-key tag) \. (k/encode-key key))))

(loop [continuation-token nil]
  (let [list-result (aws/invoke s3-client {:op :ListObjectsV2
                                           :request {:Bucket s3-bucket-name
                                                     :Prefix "M"
                                                     :ContinuationToken continuation-token}})]
    (if (anomaly? list-result)
      (throw (ex-info "failed to list S3" {:error list-result}))
      (do
        (doseq [item (->> (:Contents list-result)
                          (map :Key)
                          (remove #(string/includes? % ".")))]
          (let [new-key (rename-s3-key item)
                _ (println "copy" item "->" new-key)
                copy-result (aws/invoke s3-client {:op :CopyObject
                                                   :request {:Bucket s3-bucket-name
                                                             :CopySource (str s3-bucket-name "/" item)
                                                             :Key new-key}})]
            (if (anomaly? copy-result)
              (throw (ex-info "failed to copy object" {:error copy-result
                                                       :key item
                                                       :new-key new-key}))
              (let [_ (println "delete" (:Key item))
                    delete-result (aws/invoke s3-client {:op :DeleteObject
                                                         :request {:Bucket s3-bucket-name
                                                                   :Key item}})]
                (when (anomaly? delete-result)
                  (throw (ex-info "failed to delete object" {:error delete-result
                                                             :key item})))))))
        (when-let [t (:NextContinuationToken list-result)]
          (recur t))))))