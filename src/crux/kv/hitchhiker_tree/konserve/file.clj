(ns crux.kv.hitchhiker-tree.konserve.file
  (:require [clojure.core.async :as async]
            [konserve.cache :as c]
            [konserve.filestore :as fs]
            [konserve.serializers :as ser]))

(def file-backend
  {:start-fn (fn [_ {:keys [crux.kv/db-dir]}]
               (c/ensure-cache
                 (async/<!!
                   (fs/new-fs-store db-dir))))
   :args     {:crux.kv/db-dir {:crux.config/required? true
                               :crux.config/type      :crux.config/string}}})