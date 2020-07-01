(ns crux.kv.hitchhiker-tree.konserve.memory
  (:require [konserve.cache :as c]
            [konserve.memory :as m]
            [clojure.core.async :as async]))

(def memory-backend
  {:start-fn (fn [_ _]
               (c/ensure-cache
                 (async/<!! (m/new-mem-store))))})