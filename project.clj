(defproject jdbc-bulk-copy "0.1.0"
  :description "A clojure program to bulk update rows in postgresql table"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [seancorfield/next.jdbc "1.1.582" :exclusions [org.clojure/clojure]]
                 [org.postgresql/postgresql "42.2.16"]
                 [ring "1.8.1"]
                 [bidi "2.1.6"]
                 [mount "0.1.16"]
                 [uswitch/opencensus-clojure "0.2.93"]
                 [io.opencensus/opencensus-exporter-trace-jaeger "0.19.2"]
                 [io.opencensus/opencensus-exporter-trace-logging "0.19.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]]
  :repl-options {:init-ns jdbc-bulk-copy.core})
