(ns konserve-ddb-s3.async
  (:require [clojure.core.async.impl.protocols :as p])
  (:import (java.util.concurrent CompletableFuture)
           (java.util.function Function BiFunction)
           (java.util.concurrent.locks Lock)))

(extend-protocol p/ReadPort
  CompletableFuture
  (take! [future fn1-handler]
    (if (.isDone future)
      future
      (do
        (.handle future (reify BiFunction
                          (apply [_ result error]
                            (.lock ^Lock fn1-handler)
                            (let [get-cb (and (p/active? fn1-handler) (p/commit fn1-handler))]
                              (.unlock ^Lock fn1-handler)
                              (when get-cb
                                (get-cb (or error result)))))))
        nil))))
