(ns crux.kv.hitchhiker-tree.konserve.serializers
  (:require [konserve.protocols :as kp])
  (:import (org.fressian.handlers WriteHandler ReadHandler)
           (org.agrona DirectBuffer)
           (org.agrona.concurrent UnsafeBuffer)))

(def buffer-write-handler {"agrona-buffer"
                           (reify WriteHandler
                             (write [_ w buffer]
                               (.writeTag w "agrona-buffer" 1)
                               (let [bytes (byte-array (.capacity ^DirectBuffer buffer))]
                                 (.getBytes ^DirectBuffer buffer 0 bytes)
                                 (.writeBytes w bytes))))})

(def custom-write-handlers
  {org.agrona.DirectBuffer            buffer-write-handler
   org.agrona.MutableDirectBuffer     buffer-write-handler
   org.agrona.concurrent.AtomicBuffer buffer-write-handler
   org.agrona.concurrent.UnsafeBuffer buffer-write-handler})

(def custom-read-handlers
  {"agrona-buffer"
   (reify ReadHandler
     (read [_ r _ _]
       (let [bytes (.readObject r)]
         (UnsafeBuffer. ^"[B" bytes))))})
