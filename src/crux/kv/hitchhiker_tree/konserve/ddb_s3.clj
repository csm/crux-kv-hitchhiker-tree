(ns crux.kv.hitchhiker-tree.konserve.ddb-s3
  (:require [konserve.cache :as c]
            [konserve-ddb-s3.core :as ddb-s3]
            [konserve.serializers :as ser]
            [superv.async :as sv]))

(def ddb-s3-backend
  {:start-fn (fn [_ {::keys [table bucket region database]}]
               (c/ensure-cache
                 (sv/<?? sv/S
                         (ddb-s3/empty-store {:region         region
                                              :table          table
                                              :bucket         bucket
                                              :database       database
                                              :consistent-key #{:kv}}))))

   :args     {::table    {:crux.config/required? true
                          :crux.config/type      :crux.config/string
                          :doc                   "The DynamoDB table name."}
              ::bucket   {:crux.config/required? true
                          :crux.config/type      :crux.config/string
                          :doc                   "The S3 bucket name."}
              ::region   {:crux.config/type :crux.config/string
                          :default          "us-west-2"
                          :doc              "The AWS region name."}
              ::database {:crux.config/type :crux.config/string
                          :doc              "A database identifier (for using the same table/bucket for multiple KV stores)."}}})