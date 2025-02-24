(ns flow-storm.plugins.async-flow.runtime
  (:require [flow-storm.runtime.indexes.api :as ia]
            [flow-storm.runtime.debuggers-api :as dbg-api]
            [flow-storm.runtime.values :as rt-values]
            [clojure.core.async.impl.channels])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))



(defn- maybe-extract-thread-pid [threads-info flow-id thread-id tl-e]
  (if (and (not (contains? threads-info thread-id))
           (ia/expr-trace? tl-e)
           (= 'pid (ia/get-sub-form (ia/get-timeline flow-id thread-id) tl-e))
           (let [prev-expr (get (ia/get-timeline flow-id thread-id) (dec (ia/entry-idx tl-e)))]
             (and (ia/expr-trace? prev-expr)
                  (= 'handle-command (ia/get-sub-form (ia/get-timeline flow-id thread-id) prev-expr)))))

    (assoc threads-info thread-id (ia/get-expr-val tl-e))

    threads-info))

(defn- message-keeper [*msgs-out->thread-id flow-id tl-thread-id tl-entry]
  ;; extract from impl/proc (transform state cid msg)
  (let [timeline (ia/get-timeline flow-id tl-thread-id)
        entry-idx (ia/entry-idx tl-entry)
        msg (when (> entry-idx 2)
              (let [prev-entry      (get timeline (- entry-idx 1))
                    prev-prev-entry (get timeline (- entry-idx 2))]
                (when (and (ia/expr-trace? prev-prev-entry)
                           (ia/expr-trace? prev-entry)
                           (ia/expr-trace? tl-entry)
                           (= 'state (ia/get-sub-form timeline prev-prev-entry))
                           (= 'cid   (ia/get-sub-form timeline prev-entry))
                           (= 'msg   (ia/get-sub-form timeline tl-entry)))

                  (let [msg-ref (ia/get-expr-val tl-entry)
                        fn-call (get timeline (ia/fn-call-idx tl-entry))
                        bindings (ia/get-fn-bindings fn-call)
                        c-binding-val (some (fn [b]
                                              (when (= "c" (ia/get-bind-sym-name b))
                                                (ia/get-bind-val b)))
                                            bindings)
                        out-write-thread-id (@*msgs-out->thread-id msg-ref)
                        in-ch c-binding-val]
                    {:msg-coord {:in-ch-hash (hash in-ch)
                                 :out-write-thread-id out-write-thread-id}
                     :msg (pr-str msg-ref)
                     :msg-val-ref (rt-values/reference-value! msg-ref)
                     :idx (inc entry-idx) ;; point into user's code which should be the call to transform function
                     :thread-id tl-thread-id}))))]
    (if msg
      msg

      ;; As we see messages being written to out-ch we keep a map of the messages references and the out-chans
      ;; they went in.
      ;; This is hacky and assumes each message get written into one out-chan. It works on fan-outs because
      ;; they get copied by a mult, but the msg is put into the out-ch only by the [outc (first msgs)] instruction.
      (when (and (ia/expr-trace? tl-entry)
                 (instance? ManyToManyChannel (ia/get-expr-val tl-entry))
                 (= 'outc (ia/get-sub-form timeline tl-entry))
                 (ia/expr-trace? (get timeline (+ entry-idx 2)))
                 (= '(first msgs) (ia/get-sub-form timeline (get timeline (+ entry-idx 2)))))
        ;; if we are here we assume we are in clojure.core.async.flow.impl/send-outpus [outc (first msgs)] form
        (let [m (ia/get-expr-val (get timeline (+ entry-idx 2)))]
          (swap! *msgs-out->thread-id assoc m tl-thread-id))

        ;; return nil since this path doesn't find messages
        nil))))

;;;;;;;;;;;;;;;;;;;;;;;;
;; Plugin Runtime API ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Use an interruptible task for messages collection because this can take long
;; Also with an async task we can report messages as we find them
(defn extract-messages-task [flow-id]
  (let [*msgs-out->thread-id (atom {})]
    (dbg-api/submit-async-interruptible-batched-timelines-keep-task
     [(ia/total-order-timeline flow-id)]
     (fn [thread-id tl-entry]
       (message-keeper *msgs-out->thread-id flow-id thread-id tl-entry)))))

(defn extract-threads->processes [flow-id]
  (let [to-timeline (ia/total-order-timeline flow-id)]
    (reduce (fn [t->p tote]
              (let [tl-entry (ia/tote-entry tote)
                    tl-thread-id (ia/tote-thread-id tote)]
                (maybe-extract-thread-pid t->p flow-id tl-thread-id tl-entry)))
            {}
            to-timeline)))

(defn extract-conns [flow-id]
  (let [;; find the conn-map
        conn-map (if-let [entry (ia/find-entry-by-sub-form-pred-all-threads
                                 flow-id
                                 (fn [sf]
                                   (= 'conn-map sf)))]
                   (ia/get-expr-val entry)
                   (throw (ex-info "Can't find conn-map expression recording" {})))
        ;; find the out-chans in create-flow start
        in-chans (if-let [entry (ia/find-entry-by-sub-form-pred-all-threads
                                 flow-id
                                 (fn [sf]
                                   (and (seq? sf)
                                        (let [[a b] sf]
                                          (and (= a 'zipmap) (= b '(keys inopts)))))))]
                   (ia/get-expr-val entry)
                   (throw (ex-info "Can't find in-chans expression recording" {})))]

    (reduce (fn [conns [[out-pid :as cout] cin-set]]
              (reduce (fn [ocs cin]
                        (conj ocs {:conn [cout cin]
                                   :conn-coord [out-pid (hash (in-chans cin))]}))
                      conns
                      cin-set))
            []
            conn-map)))

(dbg-api/register-api-function :plugins.async-flow/extract-conns extract-conns)
(dbg-api/register-api-function :plugins.async-flow/extract-threads->processes extract-threads->processes)
(dbg-api/register-api-function :plugins.async-flow/extract-messages-task extract-messages-task)
