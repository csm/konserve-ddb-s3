(ns konserve-ddb-s3.repl
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.core.async :as async])
  (:import [java.nio.file Path Files Paths FileVisitResult FileVisitor]
           [java.io BufferedReader Closeable]
           [java.nio.file.attribute FileAttribute]))

(defn log-stream
  [reader level tag]
  (doto (Thread. ^Runnable (fn []
                             (loop []
                               (when-let [line (.readLine reader)]
                                 (log/logp level tag line)
                                 (recur)))))
    (.setDaemon true)
    (.start)))


(defn start-fakes3
  [^Path path]
  (log/info "starting fakes3 with path:" path)
  (let [process (-> (ProcessBuilder. ["fakes3" "-p" "0" "-r" (.getAbsolutePath (.toFile path))])
                    (.start))
        in (io/reader (.getInputStream process))
        err (io/reader (.getErrorStream process))
        port (loop []
               (when-let [line (async/alt!! (async/thread (.readLine ^BufferedReader err)) ([v] v)
                                            (async/timeout 10000) :timeout)]
                 (log/info "fakes3:" line)
                 (if (= :timeout line)
                   (throw (ex-info "timed out reading stdout from fakes3" {}))
                   (if-let [port (last (re-matches #".*port=([0-9]+).*" line))]
                     (Integer/parseInt port)
                     (recur)))))]
    (if (nil? port)
      (do
        (log/warn "failed to determine fakes3 port/failed to launch fakes3")
        (.destroy process)
        (throw (ex-info "failed to launch fakes3" {})))
      (do
        (log/debug "started fakes3 on port:" port)
        (log-stream in :info "fakes3:")
        (log-stream err :warn "fakes3:")
        {:port port :process process}))))

(defn start-dynamodb-local
  [^Path path port]
  (log/debug "starting dynamodb-local with path" path "port" port)
  (let [process (-> (ProcessBuilder. ["dynamodb-local" "-port" (str port) "-dbPath" (.getAbsolutePath (.toFile path))])
                    (.start))
        in (io/reader (.getInputStream process))
        err (io/reader (.getErrorStream process))]
    (if (loop []
          (when-let [line (async/alt!! (async/thread (.readLine in)) ([v] v)
                                       (async/timeout 10000) :timeout)]
            (log/info "dynamodb-local:" line)
            (cond
              (= :timeout line) (throw (ex-info "timed out reading stdout from dynamodb-local" {}))
              (.contains line "CorsParams:") true
              :else (recur))))
      (do
        (log-stream in :info "dynamodb-local:")
        (log-stream err :warn "dynamodb-local:")
        {:process process :port port})
      (do
        (.destroy process)
        (throw (ex-info "failed to start dynamodb-local" {:exit (.exitValue process)}))))))

(defn rm-rf
  [path]
  (Files/walkFileTree
    path
    (reify FileVisitor
      (preVisitDirectory [_ _ _] FileVisitResult/CONTINUE)
      (visitFile [_ file _]
        (.delete (.toFile file))
        FileVisitResult/CONTINUE)
      (visitFileFailed [_ _ _]
        FileVisitResult/TERMINATE)
      (postVisitDirectory [_ dir _]
        (.delete (.toFile dir))
        FileVisitResult/CONTINUE))))

(defrecord Environment [s3 ddb datadir]
  Closeable
  (close [_]
    (when-let [p (:process s3)]
      (.destroy p))
    (when-let [p (:process ddb)]
      (.destroy p))
    (rm-rf datadir)))

(def environment (atom nil))

(defn start-environment!
  []
  (let [datadir (Files/createTempDirectory (Paths/get "." (into-array String [])) ".konserve-ddb-s3.repl" (into-array FileAttribute []))
        s3path (Files/createDirectory (.resolve datadir "fakes3") (into-array FileAttribute []))
        ddbpath (Files/createDirectory (.resolve datadir "ddb") (into-array FileAttribute []))
        s3 (start-fakes3 s3path)
        ddb-port (inc (:port s3))
        ddb (try (start-dynamodb-local ddbpath ddb-port)
                 (catch Throwable t
                   (.destroy (:process s3))
                   (throw t)))
        env (->Environment s3 ddb datadir)]
    (reset! environment env)))

(.addShutdownHook
  (Runtime/getRuntime)
  (Thread. ^Runnable (fn [] (let [e @environment]
                              (when (compare-and-set! environment e nil)
                                (when (some? e)
                                  (.close e)))))))