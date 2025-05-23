(ns flow-storm.plugins.async-flow.runtime
  (:require [flow-storm.runtime.indexes.api :as ia]
            [flow-storm.runtime.debuggers-api :as dbg-api]
            [flow-storm.runtime.values :as rt-values]
            [clojure.core.async.impl.channels])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [clojure.lang PersistentQueue]))



(defn- maybe-extract-thread-pid [threads-info flow-id thread-id tl-e idx]
  (if (and (not (contains? threads-info thread-id))
           (ia/expr-trace? tl-e)
           (= 'pid (ia/get-sub-form (ia/get-timeline flow-id thread-id) idx))
           (let [prev-idx (dec idx)
                 prev-expr (get (ia/get-timeline flow-id thread-id) prev-idx)]
             (and (ia/expr-trace? prev-expr)
                  (= 'handle-command (ia/get-sub-form (ia/get-timeline flow-id thread-id) prev-idx)))))

    (assoc threads-info thread-id (ia/get-expr-val tl-e))

    threads-info))


(defn- get-in-chans-hashes [connections out-pid out-ch-obj]
  (->> connections
       (keep (fn [conn]
               (when (and (= out-pid (:out-pid conn))
                          (= (hash out-ch-obj) (:out-ch-obj-hash conn)))
                 (:in-ch-obj-hash conn))))
       (into [])))

(defn- get-coord [connections out-pid out-ch-hash in-pid in-ch-hash]
  (some (fn [c]
          (when (and (= (:out-pid c) out-pid)
                     (= (:in-pid c) in-pid)
                     (= (:in-ch-obj-hash c) in-ch-hash)
                     (= (:out-ch-obj-hash c) out-ch-hash))
            [[out-pid (:out-ch-id c)] [in-pid (:in-ch-id c)]]))
        connections))

(defn- many-to-many-chan? [obj]
  (or (instance? ManyToManyChannel obj)
      ;; this is to also support parsing flowbook replayed flows
      (and (map? obj)
           (= "clojure.core.async.impl.channels.ManyToManyChannel"
              (:flow-storm.plugins.flowbook.runtime/unserializable-obj-class-name obj)))))

(defn- message-keeper [*in-ch-queues connections threads->processes flow-id tl-thread-id entry-idx tl-entry]
  ;; extract from impl/proc (transform state cid msg)
  (try
    (let [timeline (ia/get-timeline flow-id tl-thread-id)
          msg (when (> entry-idx 2)
                (let [prev-entry-idx (- entry-idx 1)
                      prev-entry      (get timeline prev-entry-idx)
                      prev-prev-entry-idx (- entry-idx 2)
                      prev-prev-entry (get timeline prev-prev-entry-idx)]
                 (when (and (ia/expr-trace? prev-prev-entry)
                            (ia/expr-trace? prev-entry)
                            (ia/expr-trace? tl-entry)
                            (= 'state (ia/get-sub-form timeline prev-prev-entry-idx))
                            (= 'cid   (ia/get-sub-form timeline prev-entry-idx))
                            (= 'msg   (ia/get-sub-form timeline entry-idx)))

                   (let [msg-ref (ia/get-expr-val tl-entry)
                         fn-call (get timeline (ia/fn-call-idx tl-entry))
                         bindings (ia/get-fn-bindings fn-call)
                         c-binding-val (some (fn [b]
                                               (when (= "c" (ia/get-bind-sym-name b))
                                                 (ia/get-bind-val b)))
                                             bindings)
                         in-ch c-binding-val
                         in-ch-hash (hash in-ch)
                         in-pid (threads->processes tl-thread-id)
                         next-queue-msg (peek (get @*in-ch-queues in-ch-hash))]
                     (swap! *in-ch-queues update in-ch-hash pop)


                     ;; we need to skip the messages comming from outside for now,
                     ;; since the graph is not representing graph external input channels
                     (when-not (nil? (:msg next-queue-msg))

                       {:msg-coord (get-coord connections
                                              (:out-pid next-queue-msg)
                                              (:out-ch-hash next-queue-msg)
                                              in-pid
                                              in-ch-hash)
                        :msg (pr-str msg-ref)
                        :msg-val-ref (rt-values/reference-value! msg-ref)
                        :idx (inc entry-idx) ;; point into user's code which should be the call to transform function
                        :thread-id tl-thread-id})))))]
     (if msg
       msg

       ;; As we see messages being written to out-ch we keep a map of the messages references and the out-chans
       ;; they went in.
       ;; This is hacky and assumes each message get written into one out-chan. It works on fan-outs because
       ;; they get copied by a mult, but the msg is put into the out-ch only by the [outc (first msgs)] instruction.
       (when (and (ia/expr-trace? tl-entry)
                  (many-to-many-chan? (ia/get-expr-val tl-entry))
                  (= 'outc (ia/get-sub-form timeline entry-idx))
                  (ia/expr-trace? (get timeline (+ entry-idx 2)))
                  (= '(first msgs) (ia/get-sub-form timeline (+ entry-idx 2))))
         ;; if we are here we assume we are in clojure.core.async.flow.impl/send-outpus [outc (first msgs)] form
         (let [out-ch-obj (ia/get-expr-val (get timeline entry-idx))
               msg (ia/get-expr-val (get timeline (+ entry-idx 2)))
               out-pid (threads->processes tl-thread-id)
               in-chs (get-in-chans-hashes connections out-pid out-ch-obj)]

           (swap! *in-ch-queues (fn [queues]
                                  (reduce (fn [qs in-ch-hash]
                                            (update qs in-ch-hash conj {:msg msg
                                                                        :out-ch-hash (hash out-ch-obj)
                                                                        :out-pid out-pid}))
                                          queues
                                          in-chs))))

         ;; return nil since this path doesn't find messages
         nil)))
    (catch Exception e
      (.printStackTrace e))))

;;;;;;;;;;;;;;;;;;;;;;;;
;; Plugin Runtime API ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Use an interruptible task for messages collection because this can take long
;; Also with an async task we can report messages as we find them
(defn extract-messages-task [flow-id connections threads->processes]
  ;; {in-ch-hash PERSISTENT_QUEUE} of {:msg, :out-ch-hash}
  (let [*in-ch-queues (atom (reduce (fn [acc {:keys [in-ch-obj-hash]}]
                                      (assoc acc in-ch-obj-hash PersistentQueue/EMPTY))
                                    {}
                                    connections))]
    (dbg-api/submit-async-interruptible-batched-timelines-keep-task
     [(ia/total-order-timeline flow-id)]
     (fn [thread-id tl-idx tl-entry]
       (message-keeper *in-ch-queues connections threads->processes flow-id thread-id tl-idx tl-entry)))))

(defn extract-threads->processes [flow-id]
  (let [to-timeline (ia/total-order-timeline flow-id)]
    (reduce (fn [t->p tote]
              (let [tl-entry (ia/tote-entry tote)
                    tl-idx (ia/tote-timeline-idx tote)
                    tl-thread-id (ia/tote-thread-id tote)]
                (maybe-extract-thread-pid t->p flow-id tl-thread-id tl-entry tl-idx)))
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
                   (throw (ex-info "Can't find in-chans expression recording" {})))
        out-chans (if-let [entry (ia/find-entry-by-sub-form-pred-all-threads
                                  flow-id
                                  (fn [sf]
                                    (and (seq? sf)
                                         (let [[a b] sf]
                                           (and (= a 'zipmap) (= b '(keys outopts)))))))]
                    (ia/get-expr-val entry)
                    (throw (ex-info "Can't find out-chans expression recording" {})))]

    ;; here [cout cin-set] is [[:nums-gen :out-ch] #{[:num-consumer-0 :in-ch] [:num-consumer-1 :in-ch]}]
    (reduce (fn [conns [[out-pid out-ch-id :as cout] cin-set]]
              (reduce (fn [ocs [in-pid in-ch-id :as cin]]
                        (conj ocs {:out-pid out-pid
                                   :out-ch-id out-ch-id
                                   :in-pid in-pid
                                   :in-ch-id in-ch-id
                                   :in-ch-obj-hash  (hash (in-chans cin))
                                   :out-ch-obj-hash (hash (out-chans cout))}))
                      conns
                      cin-set))
            []
            conn-map)))

(dbg-api/register-api-function :plugins.async-flow/extract-conns extract-conns)
(dbg-api/register-api-function :plugins.async-flow/extract-threads->processes extract-threads->processes)
(dbg-api/register-api-function :plugins.async-flow/extract-messages-task extract-messages-task)
