(ns flow-storm.plugins.async-flow.core
  (:require [flow-storm.runtime.indexes.api :as ia]
            [flow-storm.runtime.indexes.protocols :as ip]
            [flow-storm.debugger.ui.plugins :as fs-plugins]
            [flow-storm.debugger.ui.components :as ui]
            [flow-storm.debugger.ui.utils :as ui-utils]
            [flow-storm.debugger.ui.flows.screen :refer [goto-location]]
            [flow-storm.debugger.runtime-api :as runtime-api :refer [rt-api]]
            [clojure.string :as str])
  (:import [javafx.scene.layout Priority VBox HBox]
           [com.brunomnsilva.smartgraph.graph DigraphEdgeList]
           [javafx.scene.layout Priority VBox HBox]
           [java.util.function Consumer]
           [com.brunomnsilva.smartgraph.graphview
            SmartLabelSource
            SmartGraphPanel
            ForceDirectedSpringGravityLayoutStrategy
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

(defn- maybe-extract-thread-pid [threads-info flow-id thread-id tl-e]
  (if (and (not (contains? threads-info thread-id))
           (ia/expr-trace? tl-e)
           (= 'pid (get-sub-form (ia/get-timeline flow-id thread-id) tl-e))
           (let [prev-expr (get (ia/get-timeline flow-id thread-id) (dec (ia/entry-idx tl-e)))]
             (and (ia/expr-trace? prev-expr)
                  (= 'handle-command (get-sub-form (ia/get-timeline flow-id thread-id) prev-expr)))))

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

(defn extract-messages [flow-id]
  (let [to-timeline (ia/total-order-timeline flow-id)]
    (reduce (fn [messages tote]
              (let [tl-entry (ia/tote-entry tote)
                    tl-thread-id (ia/tote-thread-id tote)
                    entry-timeline (ia/get-timeline flow-id tl-thread-id)]
                (maybe-extract-message messages tl-thread-id entry-timeline tl-entry)))
            []
            to-timeline)))


(defn extract-threads->processes [flow-id]
  (let [to-timeline (ia/total-order-timeline flow-id)]
    (reduce (fn [t->p tote]
              (let [tl-entry (ia/tote-entry tote)
                    tl-thread-id (ia/tote-thread-id tote)]
                (maybe-extract-thread-pid t->p flow-id tl-thread-id tl-entry)))
            {}
            to-timeline)))

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

(defn- build-messages-list-view []
  (ui/list-view :editable? false
                :cell-factory (fn [list-cell msg-map]
                                (-> list-cell
                                    (ui-utils/set-text (pr-str (:msg msg-map)))
                                    (ui-utils/set-graphic nil)))
                :on-click (fn [mev sel-items _]
                            (when (ui-utils/double-click? mev)
                              (let [{:keys [flow-id idx thread-id]} (first sel-items)]
                                (goto-location {:flow-id flow-id
                                                :thread-id thread-id
                                                :idx idx}))))
                :selection-mode :single
                :search-predicate (fn [msg-pprint search-str]
                                    (str/includes? msg-pprint search-str))))


(defn- build-toolbar [graph-flow-cmb messages-flow-cmb {:keys [on-graph-reload-click on-messages-reload-click]}]
  (let [reload-graph-btn (ui/icon-button
                          :icon-name "mdi-reload"
                          :on-click on-graph-reload-click
                          :tooltip "Reload the graph structure from the selected FlowStorm flow-id recordings")

        loaded-msgs-lbl (ui/label :text "")
        reload-messages-btn (ui/icon-button
                             :icon-name "mdi-reload"
                             :on-click (fn [] (on-messages-reload-click loaded-msgs-lbl))
                             :tooltip "Reload graph messages from the selected FlowStorm flow-id recordings")]
    (ui/v-box :childs [(ui/h-box :childs [(ui/label :text "Graph flow-id :") graph-flow-cmb reload-graph-btn]
                                 :spacing 5)
                       (ui/h-box :childs [(ui/label :text "Messages flow-id :") messages-flow-cmb reload-messages-btn loaded-msgs-lbl]
                                 :spacing 5)]
              :spacing 5)))

(definterface EdgeI
  (getDisplay [])
  (getEdgeChan []))

(deftype Edge [label ch]

  EdgeI
  (^{SmartLabelSource true}
   getDisplay [_] label)

  (getEdgeChan [_] ch))

(defn create-graph-pane [{:keys [on-edge-click]} in-conns]
  (let [smart-graph (DigraphEdgeList.)
        nodes (graph-nodes in-conns)]

    (doseq [n nodes]
      (.insertVertex smart-graph (pr-str n)))

    (doseq [{:keys [conn ch]} in-conns]
      (let [[[out-pid out-ch-id] [in-pid in-ch-id]] conn]
        (.insertEdge smart-graph (pr-str out-pid) (pr-str in-pid) (Edge. (format "%s -> %s" out-ch-id in-ch-id)
                                                                         ch))))

    (let [smart-graph-panel (doto (SmartGraphPanel. smart-graph
                                                    (SmartRandomPlacementStrategy.)
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

(defn- build-thread-procs-table []
  (ui/table-view
   :columns ["Process" "Thread-id"]
   :cell-factory (fn [_ txt] (ui/label :text txt))
   :resize-policy :constrained
   :selection-mode :single
   :search-predicate (fn [txt search-str]
                       (str/includes? txt search-str))))

(fs-plugins/register-plugin
 :async-flow
 {:label "Async Flow"
  :dark-css-resource  "flow-storm-async-flow-plugin/dark.css"
  :light-css-resource "flow-storm-async-flow-plugin/light.css"
  :on-focus (fn [{:keys [graph-flow-cmb messages-flow-cmb]}]
              (let [flow-ids (into #{} (map first (runtime-api/all-flows-threads rt-api)))]
                (ui-utils/combo-box-set-items graph-flow-cmb flow-ids)
                (ui-utils/combo-box-set-items messages-flow-cmb flow-ids)))
  :on-create
  (fn [_]
    (try
      (let [graph-box (ui/v-box :childs [])
            *graph-flow-id (atom 0)
            *messages-flow-id (atom 0)
            graph-flow-cmb (ui/combo-box :items []
                                         :cell-factory (fn [_ flow-id] (ui/label :text (str flow-id)))
                                         :button-factory (fn [_ flow-id] (ui/label :text (str flow-id)))
                                         :on-change (fn [_ flow-id] (reset! *graph-flow-id flow-id)))
            messages-flow-cmb (ui/combo-box :items []
                                            :cell-factory (fn [_ flow-id] (ui/label :text (str flow-id)))
                                            :button-factory (fn [_ flow-id] (ui/label :text (str flow-id)))
                                            :on-change (fn [_ flow-id] (reset! *messages-flow-id flow-id)))

            messages-list (build-messages-list-view)
            threads-procs-table (build-thread-procs-table)
            set-messages (fn [messages]
                           ((:clear messages-list))
                           ((:add-all messages-list) (mapv (fn [m] (assoc m :flow-id @*messages-flow-id)) messages)))
            set-graph-pane (fn [graph-pane]
                             (.clear (.getChildren graph-box))
                             (.addAll (.getChildren graph-box) [graph-pane]))
            *messages-by-chan (atom nil)
            toolbar-pane (build-toolbar graph-flow-cmb
                                        messages-flow-cmb
                                        {:on-graph-reload-click
                                         (fn []
                                           (let [flow-id @*graph-flow-id
                                                 in-conns (extract-in-conns flow-id)
                                                 threads->processes (extract-threads->processes flow-id)
                                                 graph-pane (create-graph-pane {:on-edge-click (fn [ch]
                                                                                                 (when-let [mbc @*messages-by-chan]
                                                                                                   (set-messages (mbc ch))))}
                                                                               in-conns)]
                                             (set-graph-pane graph-pane)
                                             ;; This is supper hacky but graph-pane init needs to run
                                             ;; after it has been render and the graph-pane has a size.
                                             ;; For now waiting a little bit after adding it to the stage works
                                             (future (Thread/sleep 500) (.init graph-pane))

                                             ;; set thread-procs-table
                                             ((:clear threads-procs-table))
                                             ((:add-all threads-procs-table) (mapv (fn [[tid pid]] [(str pid) (str tid)] ) threads->processes))))
                                         :on-messages-reload-click
                                         (fn [loaded-msgs-lbl]
                                           (try
                                             (let [messages (extract-messages @*messages-flow-id)
                                                   messages-by-chan (group-by :ch messages)]
                                               (ui-utils/set-text loaded-msgs-lbl (str "Loaded messages: " (count messages)))
                                               (reset! *messages-by-chan messages-by-chan))
                                             (catch Exception e (.printStackTrace e))))})
            messages-pane (:list-view-pane messages-list)
            threads-procs-pane (:table-view-pane threads-procs-table)
            bottom-box (ui/split :orientation :horizontal
                                 :childs [messages-pane threads-procs-pane]
                                 :sizes [0.7])]
        (VBox/setVgrow graph-box Priority/ALWAYS)
        (HBox/setHgrow graph-box Priority/ALWAYS)
        (HBox/setHgrow messages-pane Priority/ALWAYS)
        (HBox/setHgrow threads-procs-pane Priority/ALWAYS)

        {:fx/node (ui/border-pane
                   :top toolbar-pane
                   :center (ui/split :orientation :vertical
                                     :childs [graph-box
                                              bottom-box]
                                     :sizes [0.5]))
         :graph-flow-cmb graph-flow-cmb
         :messages-flow-cmb messages-flow-cmb})
      (catch Exception e
        (.printStackTrace e)
        (ui/label :text (.getMessage e)))))})

(comment


  )
