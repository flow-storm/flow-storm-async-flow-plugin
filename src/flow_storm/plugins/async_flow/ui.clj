(ns flow-storm.plugins.async-flow.ui
  (:require [flow-storm.debugger.ui.plugins :as fs-plugins]
            [flow-storm.debugger.ui.components :as ui]
            [flow-storm.debugger.ui.tasks :as tasks]
            [flow-storm.debugger.ui.utils :as ui-utils]
            [flow-storm.debugger.ui.flows.screen :refer [goto-location]]
            [flow-storm.debugger.runtime-api :as runtime-api :refer [rt-api]]
            [flow-storm.debugger.ui.data-windows.data-windows :as data-windows]
            [clojure.string :as str])
  (:import [javafx.scene.layout Priority VBox HBox]
           [com.brunomnsilva.smartgraph.graph DigraphEdgeList]
           [javafx.scene.layout Priority VBox HBox]
           [java.util.function Consumer]
           [au.com.seasoft.ham GenericGraph InteropEdge InteropNode InteropHAM]
           [com.brunomnsilva.smartgraph.graphview
            SmartLabelSource
            SmartGraphPanel
            ForceDirectedSpringGravityLayoutStrategy
            SmartPlacementStrategy]))

(defn- build-messages-list-view []
  (ui/list-view :editable? false
                :cell-factory (fn [list-cell msg-map]
                                (-> list-cell
                                    (ui-utils/set-text (:msg msg-map))
                                    (ui-utils/set-graphic nil)))
                :on-click (fn [mev sel-items _]
                            (let [msg (first sel-items)]
                              (cond

                                (= 1 (.getClickCount mev))
                                (let [{:keys [msg-val-ref]} msg]
                                  (runtime-api/data-window-push-val-data rt-api
                                                                         :plugins/core-async-flow
                                                                         msg-val-ref
                                                                         {:flow-storm.debugger.ui.data-windows.data-windows/dw-id :plugins/core-async-flow
                                                                          :flow-storm.debugger.ui.data-windows.data-windows/stack-key "Message"
                                                                          :root? true}))

                                (= 2 (.getClickCount mev))
                                (let [{:keys [flow-id idx thread-id]} msg]
                                  (goto-location {:flow-id flow-id
                                                  :thread-id thread-id
                                                  :idx idx})))))
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


(defn- ham-placement-strategy []
  (reify SmartPlacementStrategy
    (place [_ target-pane-width target-pane-height smart-graph-panel]
      (try
        (let [graph (GenericGraph/create)
              nodes (->> (.getSmartVertices smart-graph-panel)
                         (mapv (fn [vertex]
                                 (let [node-name (-> vertex .getUnderlyingVertex .element)]
                                   [node-name (InteropNode. node-name)])))
                         (into {}))
              edges (->> (.getSmartEdges smart-graph-panel)
                         (mapv (fn [edge]
                                 (let [[n1 n2] (-> edge .getUnderlyingEdge .vertices)]
                                   (InteropEdge. (nodes (.element n1)) (nodes (.element n2)))))))]

          (doseq [n (vals nodes)]
            (.addNode graph n))

          (doseq [e edges]
            (.addEdge graph e))

          (let [ham (InteropHAM/create graph 2)
                aligned-ham (InteropHAM/attemptToAlign ham 10000 false)
                {:keys [g-coords max-x max-y min-x min-y]} (reduce-kv (fn [acc n v]
                                                                        (let [nx (.getCoordinate v 1)
                                                                              ny (.getCoordinate v 2)]
                                                                          (-> acc
                                                                              (update :g-coords (fn [gc] (assoc gc (.getId n) {:x nx :y ny})))
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
                scale (/ (min target-pane-width target-pane-height) (max g-width g-height))
                min-x (* min-x scale)
                min-y (* min-y scale)
                x-trans (+ 20 (if (neg? min-x) (* -1 min-x) 0))
                y-trans (+ 20 (if (neg? min-y) (* -1 min-y) 0))
                g-coords (update-vals g-coords (fn [c]
                                                 (-> c
                                                     (update :x (fn [x] (+ x-trans (* x scale))))
                                                     (update :y (fn [y] (+ y-trans (* y scale)))))))]

            (doseq [vertex (.getSmartVertices smart-graph-panel)]
              (let [node-name (-> vertex .getUnderlyingVertex .element)
                    {:keys [x y]} (get g-coords node-name)]
                (.setPosition vertex x y)))))
        (catch Exception e (.printStackTrace e))))))

(definterface EdgeI
  (getDisplay [])
  (getEdgeConnCoord []))

(deftype Edge [label conn-coord]

  EdgeI
  (^{SmartLabelSource true}
   getDisplay [_] label)

  (getEdgeConnCoord [_] conn-coord))

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

(defn- create-graph-pane [{:keys [on-edge-click]} conns]
  (let [smart-graph (DigraphEdgeList.)
        nodes (graph-nodes conns)]

    (doseq [n nodes]
      (.insertVertex smart-graph (pr-str n)))

    (doseq [{:keys [conn conn-coord]} conns]
      (let [[[out-pid out-ch-id] [in-pid in-ch-id]] conn]
        (.insertEdge smart-graph (pr-str out-pid) (pr-str in-pid) (Edge. (format "%s -> %s" out-ch-id in-ch-id)
                                                                         conn-coord))))

    (let [smart-graph-panel (doto (SmartGraphPanel. smart-graph
                                                    (ham-placement-strategy)
                                                    (ForceDirectedSpringGravityLayoutStrategy.))
                              (.setAutomaticLayout false))]

      (VBox/setVgrow smart-graph-panel Priority/ALWAYS)
      (HBox/setHgrow smart-graph-panel Priority/ALWAYS)

      (.setEdgeDoubleClickAction smart-graph-panel
                                 (reify Consumer
                                   (accept [_ edge-line]
                                     (on-edge-click (.getEdgeConnCoord (.element (.getUnderlyingEdge edge-line)))))))
      smart-graph-panel)))

(defn- build-thread-procs-table []
  (ui/table-view
   :columns ["Process" "Thread-id"]
   :cell-factory (fn [_ txt] (ui/label :text txt))
   :resize-policy :constrained
   :selection-mode :single
   :search-predicate (fn [txt search-str]
                       (str/includes? txt search-str))))

(defn- on-create [_]
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
          *messages (atom [])
          *threads->processes (atom nil)
          toolbar-pane (build-toolbar graph-flow-cmb
                                      messages-flow-cmb
                                      {:on-graph-reload-click
                                       (fn []
                                         (reset! *messages [])
                                         (let [flow-id @*graph-flow-id
                                               conns (runtime-api/call-by-fn-key rt-api :plugins.async-flow/extract-conns [flow-id])
                                               threads->processes (runtime-api/call-by-fn-key rt-api :plugins.async-flow/extract-threads->processes [flow-id])
                                               graph-pane (create-graph-pane {:on-edge-click
                                                                              (fn [conn-coord]
                                                                                (let [messages @*messages
                                                                                      th->pid @*threads->processes
                                                                                      messages-by-chan (->> messages
                                                                                                            (mapv (fn [{:keys [msg-coord] :as m}]
                                                                                                                    (let [{:keys [in-ch-hash out-write-thread-id]} msg-coord]
                                                                                                                      (assoc m :conn-coord [(th->pid out-write-thread-id)
                                                                                                                                            in-ch-hash]))))
                                                                                                            (group-by :conn-coord))]
                                                                                  (set-messages (messages-by-chan conn-coord))))}
                                                                             conns)]
                                           (reset! *threads->processes threads->processes)
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
                                         (reset! *messages [])
                                         (tasks/submit-task runtime-api/call-by-fn-key
                                                            [:plugins.async-flow/extract-messages-task
                                                             [@*messages-flow-id]]
                                                            {:on-progress (fn [{:keys [batch]}]
                                                                            (swap! *messages (fn [msgs] (into msgs batch)))
                                                                            (ui-utils/run-later
                                                                              (ui-utils/set-text loaded-msgs-lbl (str "Messages found :" (count @*messages)))))}))})
          messages-pane (:list-view-pane messages-list)
          threads-procs-pane (:table-view-pane threads-procs-table)
          dw-pane (data-windows/data-window-pane {:data-window-id :plugins/core-async-flow})
          bottom-box (ui/split :orientation :horizontal
                               :childs [messages-pane dw-pane threads-procs-pane]
                               :sizes [0.5 0.3])]
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
      (ui/label :text (.getMessage e)))))

(defn- on-focus [{:keys [graph-flow-cmb messages-flow-cmb]}]
  (let [flow-ids (into #{} (map first (runtime-api/all-flows-threads rt-api)))]
    (ui-utils/combo-box-set-items graph-flow-cmb flow-ids)
    (ui-utils/combo-box-set-items messages-flow-cmb flow-ids)))

(fs-plugins/register-plugin
 :async-flow
 {:label "Async Flow"
  :dark-css-resource  "flow-storm-async-flow-plugin/dark.css"
  :light-css-resource "flow-storm-async-flow-plugin/light.css"
  :on-focus on-focus
  :on-create on-create})
