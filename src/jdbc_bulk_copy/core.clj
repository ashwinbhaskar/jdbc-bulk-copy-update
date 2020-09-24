(ns jdbc-bulk-copy.core
  (:require [bidi.ring :as bd]
            [ring.adapter.jetty :as jetty]
            [mount.core :as m]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.multipart-params :refer [wrap-multipart-params]]
            [opencensus-clojure.reporting.jaeger :as jaegar]
            [opencensus-clojure.reporting.logging :as l]
            [opencensus-clojure.trace :as trace]
            [next.jdbc :as j])
  (:import (org.postgresql.copy CopyManager)
           (org.eclipse.jetty.server Server)
           (java.util.concurrent CompletableFuture)))

(m/defstate ds
  :start (j/get-datasource {:jdbcUrl "jdbc:postgresql://127.0.0.1:5432/bulk_copy_update_test?user=postgres&password="})
  :stop nil)

(defn update-inventory
  [csv-file]
  (trace/span
    "update-inventory"
    (with-open [reader (clojure.java.io/reader csv-file)]
      (let [mgr                  (-> ds
                                     j/get-connection
                                     CopyManager.)
            table-name           (str "temp_table_" (inc (rand-int 10000)))
            ddl-statement-create (format "CREATE TABLE %s (item_id integer not null, quantity integer default 0, primary key (item_id))" table-name)
            copy-statement       (format "COPY %s FROM STDIN DELIMITER ',' CSV" table-name)
            update-statement     (format "UPDATE inventory i SET quantity = tt.quantity FROM %s tt WHERE i.item_id = tt.item_id" table-name)
            ddl-statement-drop   (format "DROP TABLE %s" table-name)]
        (trace/span "create-table" (j/execute! ds [ddl-statement-create]))
        (trace/span "copy" (.copyIn mgr copy-statement reader))
        (trace/span "update" (j/execute! ds [update-statement]))
        (trace/span "drop-table" (j/execute! ds [ddl-statement-drop]))))))

(defn upload-handler
  [{:keys [params] :as requests}]
  (let [{:keys [filename tempfile]} (get params "file")]
    (println (str "updating inventory of file " filename))
    (update-inventory tempfile)
    {:status 201
     :body   "update successful"}))

(def routes ["/" [["ping" {:get (constantly {:status 200
                                             :body   "pong"})}]
                  ["file" {:post (->> upload-handler
                                      (trace/span "upload-file")
                                      wrap-params
                                      wrap-multipart-params)}]
                  [true (constantly {:status 404})]]])

(m/defstate server
  :start (jetty/run-jetty (bd/make-handler routes) {:port  8080
                                                    :host  "localhost"
                                                    :join? false})
  :stop (.stop ^Server server))

(m/defstate tracer
  :start (do (opencensus-clojure.trace/configure-tracer {:probability 1.0})
             (jaegar/report "bulk-copy-update"))
  :stop (jaegar/shutdown))


(comment
  ;; start postgres datasource, http server and jaegar tracer
  (mount.core/start #'ds #'server #'tracer)

  ;; generate a master inventory file called all-inventory.csv file which contains a million inventories with their quantities
  (with-open [w (clojure.java.io/writer "/Users/ashwinbhaskar/source_code/jdbc-bulk-copy/all-inventory.csv")]
    (doseq [i (range 1 1000000)]
      (.write writer (str i "," (rand-int 1000) "\n"))))

  ;; generate files in the update_csv_files folder
  (doseq [ps (->> (range 1 1000000)
                  (partition 50000 50000))]
    (with-open [w (clojure.java.io/writer (format "/Users/ashwinbhaskar/source_code/jdbc-bulk-copy/update_csv_files/inventory-update-%d.csv" (first ps)))]
      (run! #(.write w (str % "," (rand-int 1000) "\n")) ps)))

  ;; Create table inventory
  (j/execute! ds ["CREATE TABLE inventory (item_id integer not null, quantity integer default 0, primary key (item_id))"])

  ;; Copy all items from all-inventory.csv file into inventory table
  (let [copy-statement "copy inventory from STDIN delimiter ',' CSV"
        manager        (-> (j/get-connection ds)
                           CopyManager.)]
    (with-open [r (clojure.java.io/reader "/Users/ashwinbhaskar/source_code/jdbc-bulk-copy/all-inventory.csv")]
      (.copyIn manager copy-statement r)))

  ;; update inventory concurrently using files from update_csv_files folder
  (doseq [f (-> (clojure.java.io/file "/Users/ashwinbhaskar/source_code/jdbc-bulk-copy/update_csv_files")
                file-seq)]
    (future (update-inventory f))))
