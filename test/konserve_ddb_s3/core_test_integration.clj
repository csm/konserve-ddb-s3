(ns konserve-ddb-s3.core-test-integration
  (:require [clojure.test :refer :all]
            [konserve-ddb-s3.core :as kds]
            [superv.async :as sv]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [konserve.core :as k]
            [clojure.string :as string])
  (:import (java.util UUID)
           (software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient)
           (software.amazon.awssdk.services.dynamodb.model DeleteTableRequest)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model DeleteBucketRequest)))

(def ^:dynamic *store*)

(use-fixtures
  :each
  (fn [f]
    (let [test-id (UUID/randomUUID)
          ddb-table (str "konserve-ddb-s3-test-" test-id)
          s3-bucket (str "konserve-ddb-s3-test-" test-id)
          store (sv/<?? sv/S (kds/empty-store {:region "us-west-2"
                                               :table ddb-table
                                               :bucket s3-bucket
                                               :consistent-key #(not (and (string? %) (string/starts-with? % "s3-")))}))]
      (try
        (binding [*store* store]
          (f))
        (finally
          (sv/<?? sv/S (kds/delete-store {:region "us-west-2"
                                          :table ddb-table
                                          :bucket s3-bucket}))
          (sv/<?? sv/S (.deleteTable ^DynamoDbAsyncClient (:ddb-client store)
                                     ^DeleteTableRequest (-> (DeleteTableRequest/builder)
                                                             (.tableName ddb-table)
                                                             (.build))))
          (sv/<?? sv/S (.deleteBucket ^S3AsyncClient (:s3-client store)
                                      ^DeleteBucketRequest (-> (DeleteBucketRequest/builder)
                                                               (.bucket s3-bucket)
                                                               (.build)))))))))

(deftest ^:integration atomic-ops
  (let [store *store*]
    (log/debug "store" store "locks:" (:locks store))
    (testing "that looking up nonexistent values returns false"
      (is (false? (async/<!! (k/exists? store :rabbit))))
      (is (nil? (async/<!! (k/get-in store [:rabbit])))))
    (testing "that we can assoc values"
      (is (some? (sv/<?? sv/S (k/assoc-in store [:data] {:long    42
                                                         :decimal 3.14159M
                                                         :string  "some characters"
                                                         :vector  [:foo :bar :baz/test]
                                                         :map     {:x 10 :y 10}
                                                         :set     #{:foo :bar :baz}})))))
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
    (testing "atomic ops with contention"
      (let [chans (pmap (fn [i] (k/update-in store [:data :long] (fn [n]
                                                                   (log/debug "task" i "input:" n)
                                                                   (inc n))))
                        (range 10))]
        (sv/<??* sv/S chans)
        (is (= 53 (sv/<?? sv/S (k/get-in store [:data :long]))))))

    (testing "that we can dissoc"
      (sv/<?? sv/S (k/dissoc store :data))
      (is (nil? (sv/<?? sv/S (k/get-in store [:data])))))

    (testing "that we can do ops on inconsistent keys"
      (is (false? (async/<!! (k/exists? store "s3-rabbit"))))
      (is (nil? (async/<!! (k/get-in store ["s3-rabbit"]))))
      (is (some? (sv/<?? sv/S (k/assoc-in store ["s3-data"] {:long    42
                                                             :decimal 3.14159M
                                                             :string  "some characters"
                                                             :vector  [:foo :bar :baz/test]
                                                             :map     {:x 10 :y 10}
                                                             :set     #{:foo :bar :baz}}))))
      (is (= 42 (sv/<?? sv/S (k/get-in store ["s3-data" :long]))))
      (is (= 3.14159M (sv/<?? sv/S (k/get-in store ["s3-data" :decimal]))))
      (is (= "some characters" (sv/<?? sv/S (k/get-in store ["s3-data" :string]))))
      (is (= [:foo :bar :baz/test] (sv/<?? sv/S (k/get-in store ["s3-data" :vector]))))
      (is (= {:x 10 :y 10} (sv/<?? sv/S (k/get-in store ["s3-data" :map]))))
      (is (= #{:foo :bar :baz} (sv/<?? sv/S (k/get-in store ["s3-data" :set]))))
      (is (nil? (sv/<?? sv/S (k/get-in store ["s3-data" :rabbit])))))))
