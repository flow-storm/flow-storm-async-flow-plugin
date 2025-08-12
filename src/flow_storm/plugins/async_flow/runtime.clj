(ns flow-storm.plugins.async-flow.runtime
  (:require [flow-storm.runtime.indexes.api :as ia]
            [flow-storm.runtime.debuggers-api :as dbg-api]
            [flow-storm.runtime.values :as rt-values]
            [flow-storm.runtime.indexes.protocols :as index-protos]
            [clojure.core.async.impl.channels])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [clojure.lang PersistentQueue]))

;; Move these ones to FlowStorm
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- find-binding-val [timeline tl-entry bind-name]
  (let [fn-call (get timeline (ia/fn-call-idx tl-entry))
        bindings (ia/get-fn-bindings fn-call)
        binding-val (some (fn [b]
                            (when (= bind-name (ia/get-bind-sym-name b))
                              (ia/get-bind-val b)))
                          bindings)]
    binding-val))

(defn- find-entry-by-sub-form [timeline sub-form {:keys [from-idx dir]}]
  (let [next-fn (case dir
                  :forward inc
                  :backwards dec)
        tl-cnt (count timeline)]
    (loop [i from-idx]
      (when (< i tl-cnt)
        (if (= sub-form (ia/get-sub-form timeline i))
          (get timeline i)
          (recur (next-fn i)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


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

(defn- message-keeper [*in-ch-queues connections controls->pids flow-id tl-thread-id entry-idx tl-entry]
  ;; extract from impl/proc (transform state cid msg)
  (try
    (let [timeline (ia/get-timeline flow-id tl-thread-id)
          entry-fn-call (ia/get-fn-call timeline entry-idx)
          fn-ns (ia/get-fn-ns entry-fn-call)
          fn-name (ia/get-fn-name entry-fn-call)
          msg (when (and (> entry-idx 2)
                         (= fn-ns "clojure.core.async.flow.impl")
                         (.startsWith ^String fn-name "proc"))
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
                          in-ch (ia/get-expr-val (find-entry-by-sub-form timeline 'c {:from-idx entry-idx, :dir :backwards}))
                          ctl-ch-entry (find-entry-by-sub-form timeline 'control {:from-idx entry-idx, :dir :backwards})
                          ctrl-ch-hash (hash (ia/get-expr-val ctl-ch-entry))
                          in-ch-hash (hash in-ch)
                          in-pid (controls->pids ctrl-ch-hash)
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
        (when (and (= fn-ns "clojure.core.async.flow.impl")
                   (= fn-name "send-outputs")
                   (ia/expr-trace? tl-entry)
                   (many-to-many-chan? (ia/get-expr-val tl-entry))
                   (= 'outc (ia/get-sub-form timeline entry-idx))
                   (ia/expr-trace? (get timeline (+ entry-idx 2)))
                   (= '(first msgs) (ia/get-sub-form timeline (+ entry-idx 2))))
          ;; if we are here we assume we are in clojure.core.async.flow.impl/send-outpus [outc (first msgs)] form
          (let [out-ch-obj (ia/get-expr-val (get timeline entry-idx))
                msg (ia/get-expr-val (get timeline (+ entry-idx 2)))
                ctrl-ch-hash (hash (find-binding-val timeline tl-entry "control"))
                out-pid (controls->pids ctrl-ch-hash)
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
(defn extract-messages-task [flow-id connections control-ch->pid]
  ;; {in-ch-hash PERSISTENT_QUEUE} of {:msg, :out-ch-hash}
  (let [*in-ch-queues (atom (reduce (fn [acc {:keys [in-ch-obj-hash]}]
                                      (assoc acc in-ch-obj-hash PersistentQueue/EMPTY))
                                    {}
                                    connections))]
    (dbg-api/submit-async-interruptible-batched-timelines-keep-task
     [(ia/total-order-timeline flow-id)]
     (fn [thread-id tl-idx tl-entry]
       (message-keeper *in-ch-queues connections control-ch->pid flow-id thread-id tl-idx tl-entry)))))

(defn extract-messages-sync [flow-id connections control-ch->pid]
  ;; {in-ch-hash PERSISTENT_QUEUE} of {:msg, :out-ch-hash}
  (let [*in-ch-queues (atom (reduce (fn [acc {:keys [in-ch-obj-hash]}]
                                      (assoc acc in-ch-obj-hash PersistentQueue/EMPTY))
                                    {}
                                    connections))]
    (keep (fn [tote-entry]
            (let [timeline (index-protos/tote-thread-timeline tote-entry)
                  tl-idx (index-protos/tote-thread-timeline-idx tote-entry)
                  thread-id (ia/timeline-thread-id timeline tl-idx)
                  tl-entry (get timeline tl-idx)]
              (message-keeper *in-ch-queues connections control-ch->pid flow-id thread-id tl-idx tl-entry)))
     (ia/total-order-timeline flow-id))))

(defn extract-controls->processes [timeline]
  (let [tl-cnt (count timeline)]
    (loop [idx 0
           controls->pid {}]
      (if (< idx tl-cnt)
        (let [tl-entry (get timeline idx)]
          (if (and (ia/expr-trace? tl-entry)
                   (= 'control-tap (ia/get-sub-form timeline idx))
                   (let [prev-idx (dec idx)
                         prev-expr (get timeline prev-idx)]
                     (and (ia/expr-trace? prev-expr)
                          (= 'control-mult (ia/get-sub-form timeline prev-idx)))))

            (let [pid (find-binding-val timeline tl-entry "pid")
                  ctrl-ch (ia/get-expr-val tl-entry)]
              (recur (inc idx) (assoc controls->pid (hash ctrl-ch) pid)))

            (recur (inc idx) controls->pid)))
        controls->pid))))

(defn find-connections-data [timeline]
  ;; We know all the connection data is recorded on the same timeline,
  ;; but we don't know which.
  (loop [idx 0
         data nil]
    (when (< idx (count timeline))
      (let [entry-fn-call (ia/get-fn-call timeline idx)
            fn-ns (ia/get-fn-ns entry-fn-call)
            fn-name (ia/get-fn-name entry-fn-call)]
        (if-not (and (= "clojure.core.async.flow.impl" fn-ns)
                     (= "start" fn-name))

          (recur (inc idx) data)

          (let [entry (get timeline idx)
                sub-form (ia/get-sub-form timeline idx)]
            (cond

              (and (seq? sub-form)
                   (let [[a b] sub-form]
                     (and (= a 'zipmap) (= b '(keys inopts)))))
              (recur (inc idx) (assoc data :in-chans (ia/get-expr-val entry)))

              (and (seq? sub-form)
                   (let [[a b] sub-form]
                     (and (= a 'zipmap) (= b '(keys outopts)))))
              (recur (inc idx) (assoc data :out-chans (ia/get-expr-val entry)))

              (= 'conn-map sub-form)
              (assoc data :conn-map (ia/get-expr-val entry))


              :else
              (recur (inc idx) data))))))))

(defn extract-conns [flow-id]
  (let [conn-tl (some (fn [thread-id]
                        (let [tl (ia/get-timeline flow-id thread-id)]
                          (when (->> (index-protos/all-stats tl)
                                     keys
                                     (some (fn [fn-data]
                                             (and (= (:fn-ns fn-data) "clojure.core.async.flow.impl")
                                                  (= (:fn-name fn-data) "start")))))
                            tl)))
                      (ia/all-threads-ids flow-id))
        {:keys [conn-map in-chans out-chans]} (find-connections-data conn-tl)]

    ;; here [cout cin-set] is [[:nums-gen :out-ch] #{[:num-consumer-0 :in-ch] [:num-consumer-1 :in-ch]}]
    {:conns (reduce (fn [conns [[out-pid out-ch-id :as cout] cin-set]]
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
                    conn-map)
     :control-ch->pid (extract-controls->processes conn-tl)}))

(dbg-api/register-api-function :plugins.async-flow/extract-conns extract-conns)
(dbg-api/register-api-function :plugins.async-flow/extract-messages-task extract-messages-task)


(comment
  (extract-conns 0)
  )
