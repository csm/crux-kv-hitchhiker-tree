(ns crux.kv.hitchhiker-tree
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.kv :as kv]
            crux.kv.hitchhiker-tree.konserve.memory
            [hasch.benc :as benc]
            [hitchhiker.tree :as hh]
            [hitchhiker.tree.bootstrap.konserve :as hk]
            hitchhiker.tree.key-compare
            [hitchhiker.tree.node :as n]
            [hitchhiker.tree.utils.async :as ha]
            [konserve.cache :as k]
            konserve.memory)
  (:import (java.io Closeable ByteArrayOutputStream)
           (org.agrona DirectBuffer)
           (org.agrona.io DirectBufferInputStream)
           (org.agrona.concurrent UnsafeBuffer)))

(defn- left-successor
  [path]
  (ha/go-try
    (when-let [common-parent-path
               (hh/backtrack-up-path-until path
                                           (fn [parent index]
                                             (<= 0 (dec index)
                                                   (dec (count (:children parent))))))]
      (let [next-index (-> common-parent-path peek dec)
            parent (-> common-parent-path pop peek)
            new-sibling (-> (nth (:children parent)
                                 next-index)
                            hh/<?-resolve)
            sibling-lineage (loop [res (transient [new-sibling])
                                   s new-sibling]
                              (let [first-child (-> s :children first)]
                                (if (n/address? first-child)
                                  (let [resolved-first-child (hh/<?-resolve first-child)]
                                    (when (n/address? resolved-first-child)
                                      (recur (conj! res resolved-first-child)
                                             resolved-first-child)))
                                  (persistent! res))))
            path-suffix (-> (interleave sibling-lineage
                                        (map #(dec (count (:children %))) sibling-lineage))
                            ;; butlast ensures we end w/ node
                            (butlast))]
        (-> (pop common-parent-path)
            (conj next-index)
            (into path-suffix))))))

(defn- reverse-iterator
  [path end-key inclusive?]
  (let [start-node (peek path)]
    (assert (hh/data-node? start-node))
    (let [last-elements (-> start-node
                            :children
                            (subseq (if inclusive? <= <) end-key)
                            (reverse))
          prev-elements (lazy-seq
                          (when-let [succ (left-successor (pop path))]
                            (reverse-iterator succ end-key inclusive?)))]
      (concat last-elements prev-elements))))

(defn- lookup-rev-iter
  [tree key inclusive?]
  (let [path (hh/lookup-path tree key)]
    (when path
      (reverse-iterator path key inclusive?))))

(defrecord HitchhikerTreeKVIterator [snapshot cursor]
  kv/KvIterator
  (seek [_ k]
    (let [{:keys [iter]} (vswap! cursor assoc
                                 :iter (hh/lookup-fwd-iter @(:root @snapshot) k)
                                 :forward true)]
      (some-> iter first key)))

  (next [this]
    (let [{:keys [iter]} (vswap! cursor
                                 (fn [{:keys [iter forward] :as cursor}]
                                   (if (true? forward)
                                     (assoc cursor :iter (next iter))
                                     (assoc cursor :iter (next (hh/lookup-fwd-iter @(:root @snapshot)
                                                                                   (some-> iter first key)))
                                                   :forward true))))]
      (some-> iter first key)))

  (prev [_]
    (let [{:keys [iter]} (vswap! cursor
                                 (fn [{:keys [iter forward] :as cursor}]
                                   (if (false? forward)
                                     (assoc cursor :iter (next iter))
                                     (assoc cursor :iter (lookup-rev-iter @(:root @snapshot)
                                                                          (or (some-> iter first key)
                                                                              (n/-last-key @(:root @snapshot)))
                                                                          (nil? (some-> iter first key)))
                                                   :forward false))))]
      (some-> iter first key)))

  (value [_]
    (some-> @cursor :iter first (val)))

  Closeable
  (close [_] (vreset! snapshot nil)))

(defrecord HitchhikerTreeKVSnapshot [root]
  kv/KvSnapshot
  (new-iterator [this]
    (->HitchhikerTreeKVIterator (volatile! this) (volatile! nil)))

  (get-value [_ k]
    ; hh/lookup-key fails when you have an empty tree
    (ha/<??
      (ha/go-try
        (some-> (ha/<? (hh/lookup-path @root k))
                (peek)
                (hh/<?-resolve)
                :children
                (get k)))))

  Closeable
  (close [_] (vreset! root nil)))

(defrecord HitchhikerTreeKVStore [root backend]
  kv/KvStore
  (new-snapshot [_]
    (let [r @root]
      (->HitchhikerTreeKVSnapshot (volatile! r))))

  (store [_ kvs]
    (swap! root (fn [root] (reduce (fn [tree [k v]] (hh/insert tree k v)) root kvs))))

  (delete [_ ks]
    (swap! root (fn [root] (reduce hh/delete root ks))))

  (fsync [_]
    ; todo we may want to do a merge step here, so concurrent updates
    ; can get merged into our changes.
    ;
    ; idea: we flush out our tree to the backend (keys are data-dependent,
    ; so we either write out a completely independent value, or our object
    ; is already in the backing store, so we skip writing it). Once our
    ; tree is written, we write the root node ID along with the parent root
    ; node ID, then attempt to point the "absolute root" as our value with
    ; a compare-and-set. If we can't write the root node, it means someone else
    ; came along and wrote before us. In that case, we read the new root, and
    ; then march back through the graph until we find our closest common ancestor
    ; tree, then merge each successive tree with our tree, then attempt to
    ; write the merged root again (repeat as necessary).
    ; possibly only have one writer,
    (let [tree @root]
      (ha/<?? (hh/flush-tree tree backend))
      (log/debug "flushed tree, writing root ID" (pr-str (-> tree :storage-addr async/poll!)))
      (async/<!! (k/assoc-in (:store backend) [:kv :root]
                             (-> tree :storage-addr async/poll!)))))

  (compact [_]
    :todo)

  (backup [this dir]
    (throw (UnsupportedOperationException. "not implemented")))

  (count-keys [this]
    (with-open [snap (kv/new-snapshot this)
                iter (kv/new-iterator snap)]
      (let [first-key (kv/seek iter nil)]
        (if (nil? first-key)
          0
          (loop [count 1]
            (if (kv/next iter)
              (recur (inc count))
              count))))))

  (db-dir [_] nil)

  (kv-name [_]
    "hitchhiker-tree"))

(extend-protocol benc/PHashCoercion
  DirectBuffer
  (-coerce [this md-create-fn write-handlers]
    (let [bout (ByteArrayOutputStream. (.capacity this))
          bufin (DirectBufferInputStream. this)]
      (io/copy bufin bout)
      (let [bytes (.toByteArray bout)]
        (benc/-coerce bytes md-create-fn write-handlers)))))

(extend-protocol hitchhiker.tree.node/IEDNOrderable
  DirectBuffer
  (-order-on-edn-types [_] -10))

(def kv
  {:crux.node/kv-store {:start-fn (fn [{::keys [backend]} {::keys [index-buffer-size data-buffer-size op-buffer-size]}]
                                    (let [root (if-let [root-address (async/<!! (k/get-in (:store backend) [:kv :root]))]
                                                 (if (instance? Throwable root-address)
                                                   (throw root-address)
                                                   (ha/<?? (hk/create-tree-from-root-key (:store backend) root-address)))
                                                 (ha/<?? (hh/b-tree (hh/->Config index-buffer-size data-buffer-size op-buffer-size))))]
                                      (log/debug "resolved root node: " root)
                                      (->HitchhikerTreeKVStore (atom root) backend)))
                        :args     {::index-buffer-size {:doc              "Size of each index node"
                                                        :default          32
                                                        :crux.config/type :crux.config/nat-int}
                                   ::data-buffer-size  {:doc              "Size of each data node"
                                                        :default          64
                                                        :crux.config/type :crux.config/nat-int}
                                   ::op-buffer-size    {:doc              "Size of each op buffer"
                                                        :default          128
                                                        :crux.config/type :crux.config/nat-int}}
                        :deps     #{::backend}}
   ::backend           {:start-fn (fn [{::keys [konserve]} _]
                                    (log/debug "bootstrapping hh-tree backend with konserve:" konserve)
                                    (hk/->KonserveBackend (hk/add-hitchhiker-tree-handlers konserve)))
                        :deps     #{::konserve}}
   ::konserve          crux.kv.hitchhiker-tree.konserve.memory/memory-backend})