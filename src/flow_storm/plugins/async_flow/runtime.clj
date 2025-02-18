(ns flow-storm.plugins.async-flow.runtime
  (:require [flow-storm.runtime.indexes.api :as ia]
            [flow-storm.runtime.debuggers-api :as dbg-api]))



(defn- maybe-extract-thread-pid [threads-info flow-id thread-id tl-e]
  (if (and (not (contains? threads-info thread-id))
           (ia/expr-trace? tl-e)
           (= 'pid (ia/get-sub-form (ia/get-timeline flow-id thread-id) tl-e))
           (let [prev-expr (get (ia/get-timeline flow-id thread-id) (dec (ia/entry-idx tl-e)))]
             (and (ia/expr-trace? prev-expr)
                  (= 'handle-command (ia/get-sub-form (ia/get-timeline flow-id thread-id) prev-expr)))))

    (assoc threads-info thread-id (ia/get-expr-val tl-e))

    threads-info))

(defn- message-keeper [flow-id tl-thread-id tl-entry]
  ;; extract from impl/proc (transform state cid msg)
  (let [timeline (ia/get-timeline flow-id tl-thread-id)
        entry-idx (ia/entry-idx tl-entry)]
    (when (> entry-idx 2)
      (let [prev-entry      (get timeline (- entry-idx 1))
            prev-prev-entry (get timeline (- entry-idx 2))]
        (when (and (ia/expr-trace? prev-prev-entry)
                   (ia/expr-trace? prev-entry)
                   (ia/expr-trace? tl-entry)
                   (= 'state (ia/get-sub-form timeline prev-prev-entry))
                   (= 'cid   (ia/get-sub-form timeline prev-entry))
                   (= 'msg   (ia/get-sub-form timeline tl-entry)))

          (let [msg (ia/get-expr-val tl-entry)
                fn-call (get timeline (ia/fn-call-idx tl-entry))
                bindings (ia/get-fn-bindings fn-call)
                c-binding-val (some (fn [b]
                                      (when (= "c" (ia/get-bind-sym-name b))
                                        (ia/get-bind-val b)))
                                    bindings)]
            {:ch-hash (hash c-binding-val)
             :msg (pr-str msg)
             :idx (inc entry-idx) ;; point into user's code which should be the call to transform function
             :thread-id tl-thread-id}))))))

;;;;;;;;;;;;;;;;;;;;;;;;
;; Plugin Runtime API ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Use an interruptible task for messages collection because this can take long
;; Also with an async task we can report messages as we find them
(defn extract-messages-task [flow-id]
  (dbg-api/submit-async-interruptible-batched-timelines-keep-task
   (ia/timelines-for {:flow-id flow-id})
   (fn [thread-id tl-entry]
     (message-keeper flow-id thread-id tl-entry))))

(defn extract-threads->processes [flow-id]
  (let [to-timeline (ia/total-order-timeline flow-id)]
    (reduce (fn [t->p tote]
              (let [tl-entry (ia/tote-entry tote)
                    tl-thread-id (ia/tote-thread-id tote)]
                (maybe-extract-thread-pid t->p flow-id tl-thread-id tl-entry)))
            {}
            to-timeline)))

(defn extract-in-conns [flow-id]
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

    (reduce (fn [conns [cout cin-set]]
              (reduce (fn [ocs cin]
                        (conj ocs {:conn [cout cin]
                                   :ch-hash (hash (in-chans cin))}))
                      conns
                      cin-set))
            []
            conn-map)))
