(defproject crux-hitchhiker-tree "0.1.0-SNAPSHOT"
  :description "Crux KV store on hitchhiker-tree"
  :url "http://example.com/FIXME"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "20.06-1.9.1-beta"]
                 [io.replikativ/hitchhiker-tree "0.1.7"]]
  :repl-options {:init-ns crux-hitchhiker-tree.repl})
