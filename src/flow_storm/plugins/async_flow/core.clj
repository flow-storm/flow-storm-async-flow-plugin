(ns flow-storm.plugins.async-flow.core
  (:require [flow-storm.runtime.indexes.api :as ia]
            [flow-storm.runtime.indexes.protocols :as ip]
            [flow-storm.debugger.ui.plugins :as fs-plugins]
            [flow-storm.debugger.ui.components :as ui]
            [flow-storm.debugger.ui.utils :as ui-utils]
            [flow-storm.debugger.ui.flows.screen :refer [goto-location]]
            [clojure.string :as str])
  (:import [javafx.scene.layout Priority VBox HBox]
           [javafx.scene Node]
           [javafx.scene.control Label]
           [javafx.scene.layout Pane StackPane ]
           [javafx.scene.shape Path Circle Line]
           [au.com.seasoft.ham GenericGraph InteropEdge InteropNode InteropHAM]

           [com.brunomnsilva.smartgraph.graph DigraphEdgeList]
           [javafx.scene.layout Priority VBox HBox]
           [java.util.function Consumer]
           [com.brunomnsilva.smartgraph.graphview
            SmartLabelSource
            SmartGraphPanel
            ForceDirectedSpringGravityLayoutStrategy
            SmartCircularSortedPlacementStrategy
            SmartGraphProperties
            SmartRandomPlacementStrategy]))

(defn get-sub-form [timeline tl-entry]
  (let [fn-call-entry (get timeline (ia/fn-call-idx tl-entry))
        form-id (ip/get-form-id fn-call-entry)
        expr-coord (when (or (ia/expr-trace? tl-entry)
                             (ia/fn-end-trace? tl-entry))
                     (ia/get-coord-vec tl-entry))
        form (:form/form (ia/get-form form-id))]
    (if expr-coord
      (ia/get-sub-form-at-coord form expr-coord)
      form)))

(defn- maybe-extract-thread-pid [threads-info thread-id tl-e]
  (if (and (not (contains? threads-info thread-id))
           (ia/expr-trace? tl-e)
           (= 'pid (get-sub-form (ia/get-timeline thread-id) tl-e)))
    (assoc threads-info thread-id (ia/get-expr-val tl-e))
    threads-info))

(defn- maybe-extract-message [messages tl-thread-id timeline tl-entry]
  ;; extract from impl/proc (transform state cid msg)
  (let [entry-idx (ia/entry-idx tl-entry)]
    (if (> entry-idx 2)
      (let [prev-entry      (get timeline (- entry-idx 1))
            prev-prev-entry (get timeline (- entry-idx 2))]
        (if (and (ia/expr-trace? prev-prev-entry)
                 (ia/expr-trace? prev-entry)
                 (ia/expr-trace? tl-entry)
                 (= 'state (get-sub-form timeline prev-prev-entry))
                 (= 'cid   (get-sub-form timeline prev-entry))
                 (= 'msg   (get-sub-form timeline tl-entry)))

          (let [msg (ia/get-expr-val tl-entry)
                fn-call (get timeline (ia/fn-call-idx tl-entry))
                bindings (ia/get-fn-bindings fn-call)
                c-binding-val (some (fn [b]
                                      (when (= "c" (ia/get-bind-sym-name b))
                                        (ia/get-bind-val b)))
                               bindings)]
            (conj messages {:ch c-binding-val
                            :msg msg
                            :idx (inc entry-idx) ;; point into user's code which should be the call to transform function
                            :thread-id tl-thread-id}))

          messages))
      messages)))

(defn extract-flow [flow-id]
  (let [to-timeline (ia/total-order-timeline flow-id)]
    (reduce (fn [data tote]
              (let [tl-entry (ia/tote-entry tote)
                    tl-thread-id (ia/tote-thread-id tote)
                    entry-timeline (ia/get-timeline tl-thread-id)]

                (-> data
                    (update :threads->processes maybe-extract-thread-pid tl-thread-id tl-entry)
                    (update :messages maybe-extract-message tl-thread-id entry-timeline tl-entry))))
            {:threads->processes {}
             :messages []}
            to-timeline)))

(comment
  (extract-flow 0)
  )

(defn find-entry-by-sub-form-pred [timeline pred]
  (some (fn [tl-entry]
          (let [sub-form (get-sub-form timeline tl-entry)]
            (when (pred sub-form)
              tl-entry)))
        timeline))

(defn find-entry-by-sub-form-pred-all-threads [flow-id pred]
  (some (fn [thread-id]
          (find-entry-by-sub-form-pred (ia/get-timeline flow-id thread-id) pred))
        (ia/all-threads-ids flow-id)))

(comment
  (find-entry-by-sub-form-pred (ia/get-timeline 0 29)
                               (fn [sf]
                                 (and (seq? sf)
                                      (let [[a b] sf]
                                        (and (= a 'zipmap) (= b '(keys outopts)))))))

  (find-entry-by-sub-form-pred-all-threads
   0
   (fn [sf]
     (and (seq? sf)
          (let [[a b] sf]
            (and (= a 'zipmap) (= b '(keys outopts))))))))

;; TODO:
;; - Extract in connections instead of out
;; - Extract messages from impl/proc (transform state cid *msg*) grabbing the message and taking the *c* binding as the ch
;; - We can check back once for each msg form that prevs are cid, state, transform
;; - The next index will contain the processing/transform on user's code
(comment
  (ia/as-immutable (get (ia/get-timeline 0 96) 879))
  )

(defn extract-in-conns [flow-id]
  (let [;; find the conn-map
        conn-map (if-let [entry (find-entry-by-sub-form-pred-all-threads
                                 flow-id
                                 (fn [sf]
                                   (= 'conn-map sf)))]
                   (ia/get-expr-val entry)
                   (throw (ex-info "Can't find conn-map expression recording" {})))
        ;; find the out-chans in create-flow start
        in-chans (if-let [entry (find-entry-by-sub-form-pred-all-threads
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
                                   :ch (in-chans cin)}))
                      conns
                      cin-set))
            []
            conn-map)))

(defn- graph-nodes
  "Given a conns vector, which takes the form of
  [{:conn [[:from-pid :out-ch-id] [:to-pid :in-ch-id]]} ...]
  returns a set of all pids found."
  [conns]
  (reduce (fn [nodes {:keys [conn]}]
            (let [[[from _] [to _]] conn]
              (into nodes [from to])))
          #{}
          conns))

(defn- build-messages-list-view [flow-id]
  (ui/list-view :editable? false
                :cell-factory (fn [list-cell msg-map]
                                (-> list-cell
                                    (ui-utils/set-text (pr-str (:msg msg-map)))
                                    (ui-utils/set-graphic nil)))
                :on-click (fn [mev sel-items _]
                            (when (ui-utils/double-click? mev)
                              (let [{:keys [idx thread-id]} (first sel-items)]
                                (goto-location {:flow-id flow-id
                                                :thread-id thread-id
                                                :idx idx}))))
                :selection-mode :single
                :search-predicate (fn [msg-pprint search-str]
                                    (str/includes? msg-pprint search-str))))


(defn- build-toolbar [flow-id {:keys [on-refresh]}]
  (let [refresh-btn (ui/icon-button
                     :icon-name "mdi-reload"
                     :on-click on-refresh
                     :tooltip "")]
    (ui/h-box :childs [refresh-btn])))

(defn create-graph-pane [{:keys [on-edge-click]} in-conns]
  (let [graph (GenericGraph/create)
        nodes (graph-nodes in-conns)
        inter-nodes (reduce (fn [m pid]
                              (assoc m pid (InteropNode. (name pid))))
                            {}
                            nodes)]
    (doseq [pid nodes]
      (.addNode graph (inter-nodes pid)))

    (doseq [{:keys [conn]} in-conns]
      (let [[[out-pid out-ch-id] [in-pid in-ch-id]] conn]
        (.addEdge graph (InteropEdge. (inter-nodes out-pid) (inter-nodes in-pid)))))

    (let [ham (InteropHAM/create graph 2)
          aligned-ham (InteropHAM/attemptToAlign ham 1000 false)
          {:keys [g-coords max-x max-y min-x min-y]} (reduce-kv (fn [acc n v]
                                                                  (let [nx (.getCoordinate v 1)
                                                                        ny (.getCoordinate v 2)]
                                                                    (-> acc
                                                                        (update :g-coords (fn [gc] (assoc gc (keyword (.getId n)) {:x nx :y ny})))
                                                                        (update :max-x max nx)
                                                                        (update :max-y max ny)
                                                                        (update :min-x min nx)
                                                                        (update :min-y min ny))))
                                                                {:g-coords {}
                                                                 :max-x Long/MIN_VALUE
                                                                 :max-y Long/MIN_VALUE
                                                                 :min-x Long/MAX_VALUE
                                                                 :min-y Long/MAX_VALUE}
                                                                (.getCoordinates aligned-ham))

          g-width (- max-x min-x)
          g-height (- max-y min-y)
          scale (/ 800 (max g-width g-height))
          min-x (* min-x scale)
          min-y (* min-y scale)
          x-trans (+ 20 (if (neg? min-x) (* -1 min-x) 0))
          y-trans (+ 20 (if (neg? min-y) (* -1 min-y) 0))
          g-coords (update-vals g-coords (fn [c]
                                           (-> c
                                               (update :x (fn [x] (+ x-trans (* x scale))))
                                               (update :y (fn [y] (+ y-trans (* y scale)))))))
          graph-pane (Pane.)
          fx-verts (mapv (fn [[nid {:keys [x y]}]]
                           (let [circle (Circle. 0 0 10)
                                 lbl (Label. (name nid))
                                 v (doto (StackPane. (into-array Node [circle lbl]))
                                     (.setLayoutX x)
                                     (.setLayoutY y))]
                             v))
                         g-coords)
          fx-edges (mapv (fn [{:keys [conn ch]}]
                           (let [[[out-pid out-ch-id] [in-pid in-ch-id]] conn]
                             (doto (Line. (get-in g-coords [out-pid :x]) (get-in g-coords [out-pid :y])
                                          (get-in g-coords [in-pid  :x]) (get-in g-coords [in-pid  :y]))
                               (.setStrokeWidth 5)
                               (.setOnMouseClicked
                                (ui-utils/event-handler [mev] (on-edge-click ch))))))
                         in-conns)]

      (.addAll (.getChildren graph-pane) fx-edges)
      (.addAll (.getChildren graph-pane) fx-verts)

      (VBox/setVgrow graph-pane Priority/ALWAYS)
      (HBox/setHgrow graph-pane Priority/ALWAYS)

      graph-pane)))

(definterface EdgeI
  (getDisplay [])
  (getEdgeChan []))

(deftype Edge [label ch]

  EdgeI
  (^{SmartLabelSource true}
   getDisplay [_] label)

  (getEdgeChan [_] ch))

(defn create-graph-pane2 [{:keys [on-edge-click]} in-conns]
  (let [smart-graph (DigraphEdgeList.)
        nodes (graph-nodes in-conns)]

    (doseq [n nodes]
      (.insertVertex smart-graph (pr-str n)))

    (doseq [{:keys [conn ch]} in-conns]
      (let [[[out-pid out-ch-id] [in-pid in-ch-id]] conn]
        (.insertEdge smart-graph (pr-str out-pid) (pr-str in-pid) (Edge. (format "%s -> %s" out-ch-id in-ch-id)
                                                                         ch))))

    (let [smart-graph-panel (doto (SmartGraphPanel. smart-graph
                                                    (SmartCircularSortedPlacementStrategy.)
                                                    (ForceDirectedSpringGravityLayoutStrategy.))
                              (.setAutomaticLayout false)
                              (.setPrefHeight 1000)
                              (.setPrefWidth 1000))]

      (VBox/setVgrow smart-graph-panel Priority/ALWAYS)
      (HBox/setHgrow smart-graph-panel Priority/ALWAYS)

      (.setEdgeDoubleClickAction smart-graph-panel
                                 (reify Consumer
                                   (accept [_ edge-line]
                                     (on-edge-click (.getEdgeChan (.element (.getUnderlyingEdge edge-line)))))))
      smart-graph-panel)))

(fs-plugins/register-plugin
 :async-flow
 {:label "Async Flow"
  :dark-css-resource  "flow-storm-async-flow-plugin/dark.css"
  :light-css-resource "flow-storm-async-flow-plugin/light.css"
  :on-create
  (fn [_]
    (try
      (let [graph-box (ui/v-box :childs [])
            flow-id 0
            {:keys [list-view-pane clear add-all]} (build-messages-list-view flow-id)
            set-messages (fn [messages]
                           (clear)
                           (add-all messages))
            set-graph-pane (fn [graph-pane]
                             (.clear (.getChildren graph-box))
                             (.addAll (.getChildren graph-box) [graph-pane]))
            toolbar (build-toolbar flow-id
                                   {:on-refresh
                                    (fn []
                                      (try
                                        (let [in-conns (extract-in-conns flow-id)
                                              {:keys [messages threads->processes]} (extract-flow flow-id)
                                              messages-by-chan (group-by :ch messages)
                                              graph-pane (create-graph-pane2 {:on-edge-click (fn [ch] (set-messages (messages-by-chan ch)))}
                                                                             in-conns)]
                                          (set-graph-pane graph-pane)
                                          (future (Thread/sleep 1000) (.init graph-pane)) ;; fix thissssssssssssssssss
                                          )
                                        (catch Exception e (.printStackTrace e))))})]
        (VBox/setVgrow graph-box Priority/ALWAYS)
        (HBox/setHgrow graph-box Priority/ALWAYS)

        {:fx/node (ui/border-pane
                   :top toolbar
                   :center (ui/split :orientation :vertical
                                     :childs [graph-box list-view-pane]
                                     :sizes [0.5]))})
      (catch Exception e
        (.printStackTrace e)
        (Label. (.getMessage e)))))})

(comment


  )
