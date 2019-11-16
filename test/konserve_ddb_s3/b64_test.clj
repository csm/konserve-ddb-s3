(ns konserve-ddb-s3.b64-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [konserve-ddb-s3.b64 :as b64]
            [clojure.tools.logging :as log]
            [clojure.string :as string]))

(defn compare-bytes
  [b1 b2]
  (let [diffs (map compare (map #(bit-and % 0xFF) b1) (map #(bit-and % 0xFF) b2))]
    (if (every? zero? diffs)
      (cond (< (count b1) (count b2))
            -1
            (< (count b2) (count b1))
            1
            :else
            0)
      (first (remove zero? diffs)))))

(defmethod print-method (type (byte-array 0))
  [this w]
  (.write w (str "#bytes\"" (->> this (map #(format "%02x" %)) (string/join)) \")))

(defn compare-encoded
  [v1 v2]
  (let [e1 (b64/b64-encode v1)
        e2 (b64/b64-encode v2)]
    (log/debug "v1:" v1 "e1:" (pr-str e1) "v2:" v2 "e2:" (pr-str e2))
    (compare e1 e2)))

(s/fdef compare-encoded
  :args (s/cat :v1 bytes? :v2 bytes?)
  :ret integer?
  :fn (fn [{:keys [args ret] :as t}]
        (log/debug "t:" t)
        (let [expected (compare-bytes (:v1 args) (:v2 args))]
          (or (and (neg? expected)
                   (neg? ret))
              (and (pos? expected)
                   (pos? ret))
              (and (zero? expected)
                   (zero? ret))))))

(deftest test-sortable-base64
  (testing "that sortable base64 encodings preserve order"
    (let [result (stest/check `compare-encoded)]
      (println "result:" result)
      (is (true? (-> result first :clojure.spec.test.check/ret :pass?))))))