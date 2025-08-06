(ns flow-storm.plugins.async-flow.runtime-test
  (:require [flow-storm.plugins.async-flow.all]
            [flow-storm.plugins.async-flow.runtime :as sut]
            [clojure.test :as t]
            [clojure.core.async.flow :as flow]
            [clojure.core.async :as async]))

(comment
  ;; ONE TO ONE
  (let [in-ch (async/chan)
        fl (flow/create-flow
            {:procs
             {:A {:proc (flow/process (fn A
                                        ([] {:outs {:out-1 ""}})
                                        ([{:keys [in-ch]}] {::flow/in-ports {:in-1 in-ch}})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         [state {:out-1 [(* msg 2)]}])))
                  :args {:in-ch in-ch}}
              :B {:proc (flow/process (fn B
                                        ([] {:ins {:in-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (println "Msg " msg))))}}

             :conns
             [[[:A :out-1] [:B :in-1]]]})]
    (flow/start fl)
    (flow/resume fl)

    (async/>!! in-ch 0)
    (async/>!! in-ch 1))

  (let [{:keys [conns control-ch->pid]} (sut/extract-conns 0)]
    (def conns conns)
    (def control-ch->pid control-ch->pid))

  (sut/extract-messages-sync 0 conns control-ch->pid)

  )


(comment
  ;; FAN OUT FAN IN
  (let [in-ch (async/chan)
        fl (flow/create-flow
            {:procs
             {:A {:proc (flow/process (fn A
                                        ([] {:outs {:out-1 ""}})
                                        ([{:keys [in-ch]}] {::flow/in-ports {:in-1 in-ch}})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         [state {:out-1 [(* msg 2)]}])))
                  :args {:in-ch in-ch}}
              :B {:proc (flow/process (fn B
                                        ([] {:ins {:in-1 ""}
                                             :outs {:out-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         [state {:out-1 [[:b-wrapped msg]]}])))}
              :C {:proc (flow/process (fn C
                                        ([] {:ins {:in-1 ""}
                                             :outs {:out-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         [state {:out-1 [[:c-wrapped msg]]}])))}
              :D {:proc (flow/process (fn D
                                        ([] {:ins {:in-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (println "D Msg " msg))))}}

             :conns
             [[[:A :out-1] [:B :in-1]]
              [[:A :out-1] [:C :in-1]]
              [[:B :out-1] [:D :in-1]]
              [[:C :out-1] [:D :in-1]]]})]
    (flow/start fl)
    (flow/resume fl)

    (async/>!! in-ch 0)
    (async/>!! in-ch 1))

  (let [{:keys [conns control-ch->pid]} (sut/extract-conns 0)]
    (def conns conns)
    (def control-ch->pid control-ch->pid))

  (sut/extract-messages-sync 0 conns control-ch->pid)

  )

(comment
  ;; Multi Out
  (let [in-ch (async/chan)
        fl (flow/create-flow
            {:procs
             {:A {:proc (flow/process (fn A
                                        ([] {:outs {:evens ""
                                                    :odds ""}})
                                        ([{:keys [in-ch]}] {::flow/in-ports {:in-1 in-ch}})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (if (even? msg)
                                           [state {:evens [msg]}]
                                           [state {:odds [msg]}]))))
                  :args {:in-ch in-ch}}
              :B {:proc (flow/process (fn B
                                        ([] {:ins {:in-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (println "B Msg " msg))))}
              :C {:proc (flow/process (fn C
                                        ([] {:ins {:in-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (println "C Msg " msg))))}}

             :conns
             [[[:A :evens] [:B :in-1]]
              [[:A :odds]  [:C :in-1]]]})]
    (flow/start fl)
    (flow/resume fl)

    (async/>!! in-ch 0)
    (async/>!! in-ch 1)
    (async/>!! in-ch 2)
    (async/>!! in-ch 3))

  (let [{:keys [conns control-ch->pid]} (sut/extract-conns 0)]
    (def conns conns)
    (def control-ch->pid control-ch->pid))

  (sut/extract-messages-sync 0 conns control-ch->pid)

  )


(comment
  ;; Multi IN
  (let [in-ch (async/chan)
        fl (flow/create-flow
            {:procs
             {:A {:proc (flow/process (fn A
                                        ([] {:outs {:evens ""
                                                    :odds ""}})
                                        ([{:keys [in-ch]}] {::flow/in-ports {:in-1 in-ch}})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (if (even? msg)
                                           [state {:evens [msg]}]
                                           [state {:odds [msg]}]))))
                  :args {:in-ch in-ch}}
              :B {:proc (flow/process (fn B
                                        ([] {:ins {:in-1 ""}
                                             :outs {:out-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         [state {:out-1 [[:b-wrapped msg]]}])))}
              :C {:proc (flow/process (fn C
                                        ([] {:ins {:in-1 ""}
                                             :outs {:out-1 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         [state {:out-1 [[:c-wrapped msg]]}])))}
              :D {:proc (flow/process (fn D
                                        ([] {:ins {:in-1 "", :in-2 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (println "D Msg " msg "via" ch-id))))}}

             :conns
             [[[:A :evens] [:B :in-1]]
              [[:A :odds]  [:C :in-1]]
              [[:B :out-1] [:D :in-1]]
              [[:C :out-1] [:D :in-2]]]})]
    (flow/start fl)
    (flow/resume fl)

    (async/>!! in-ch 0)
    (async/>!! in-ch 1)
    (async/>!! in-ch 2)
    (async/>!! in-ch 3))

  (let [{:keys [conns control-ch->pid]} (sut/extract-conns 0)]
    (def conns conns)
    (def control-ch->pid control-ch->pid))

  (sut/extract-messages-sync 0 conns control-ch->pid)

  )



(comment
  ;; Double bounded processes
  (let [in-ch (async/chan)
        fl (flow/create-flow
            {:procs
             {:A {:proc (flow/process (fn A
                                        ([] {:outs {:evens ""
                                                    :odds ""}})
                                        ([{:keys [in-ch]}] {::flow/in-ports {:in-1 in-ch}})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (if (even? msg)
                                           [state {:evens [msg]}]
                                           [state {:odds [msg]}]))))
                  :args {:in-ch in-ch}}
              :B {:proc (flow/process (fn B
                                        ([] {:ins {:in-1 ""
                                                   :in-2 ""}})
                                        ([_] {})
                                        ([state _] state)
                                        ([state ch-id msg]
                                         (println "Got " msg "via" ch-id)
                                         [state nil])))}
              }

             :conns
             [[[:A :evens] [:B :in-1]]
              [[:A :odds]  [:B :in-2]]]})]
    (flow/start fl)
    (flow/resume fl)

    (async/>!! in-ch 0)
    (async/>!! in-ch 1)
    (async/>!! in-ch 2)
    (async/>!! in-ch 3))

  (let [{:keys [conns control-ch->pid]} (sut/extract-conns 0)]
    (def conns conns)
    (def control-ch->pid control-ch->pid))

  (sut/extract-messages-sync 0 conns control-ch->pid)

  )


(comment
  (-> (clojure.lang.PersistentQueue/EMPTY)
      (conj 1)
      (conj 2))
  (defmethod print-method clojure.lang.PersistentQueue [q ^java.io.Writer w]
    (.write w (str (seq q))))


  (let [{:keys [conns control-ch->pid]} (sut/extract-conns 0)]
    (def conns conns)
    (def control-ch->pid control-ch->pid))

  (sut/extract-messages-sync 1 conns control-ch->pid)
  )
