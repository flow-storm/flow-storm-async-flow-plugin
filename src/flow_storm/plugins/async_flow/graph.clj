(ns flow-storm.plugins.async-flow.graph
  (:import [org.graphstream.graph.implementations SingleGraph]
           [org.graphstream.ui.fx_viewer FxViewer]
           [org.graphstream.ui.view Viewer Viewer$ThreadingModel Viewer$CloseFramePolicy]
           [org.graphstream.ui.javafx FxGraphRenderer]
           [org.graphstream.ui.fx_viewer FxDefaultView]
           [org.graphstream.ui.fx_viewer.util FxMouseManager]
           [org.graphstream.ui.view ViewerListener]
           [org.graphstream.ui.view.util InteractiveElement]
           [java.util EnumSet]))

(defn add-nodes-and-edges [graph]
  (doseq [i (range 5)]
    (let [node (.addNode graph (str "N" i))]
      (.setAttribute node "ui.label" (into-array String [(str "Node " i)]))))
  (doseq [[a b] [["N0" "N1"] ["N0" "N2"] ["N1" "N3"] ["N2" "N4"] ["N3" "N4"]]]
    (let [edge (.addEdge graph (str a "-" b) a b)]
      (.setAttribute edge "ui.label" (into-array String [(str a "-" b)])))))

(defn on-edge-click [edge-id]
  (println "Clicked edge:" edge-id))

(def loop (atom true))

(defn setup-ui []
  (let [graph (doto (SingleGraph. "Interactive Graph")
                (.setAttribute "ui.stylesheet" (into-array String ["node { fill-color: blue; size: 20px; } edge { fill-color: black; size: 10px;}"])))
        viewer (doto (FxViewer. graph Viewer$ThreadingModel/GRAPH_IN_GUI_THREAD)
                 (.enableAutoLayout))
        view (.addDefaultView viewer false)
        viewer-pipe (.newViewerPipe viewer)
        *running (atom true)]

    (-> viewer
        .getDefaultView
        (.setMouseManager (FxMouseManager. (EnumSet/of InteractiveElement/EDGE InteractiveElement/NODE InteractiveElement/SPRITE))))

    ;; (.init mouse-manager graph view)


    ;; (.setMouseManager view mouse-manager)
    (.setCloseFramePolicy viewer Viewer$CloseFramePolicy/HIDE_ONLY)
    ;;(.addView viewer "Interactive View" view)

    (add-nodes-and-edges graph)
    (.addViewerListener viewer-pipe
                        (reify ViewerListener
                          (viewClosed [_ _] (println "Closing") (reset! *running false))
                          (buttonPushed [_ id]  (println "buttonPushed" id))
                          (buttonReleased [_ id] (println "buttonReleased" id))
                          (mouseLeft [_ id] (println "mouseLeft" id))
                          (mouseOver [_ id] (println "mouseOver" id))))

    (.start
     (Thread.
      (fn []
        (while @*running
          (println "Pumping")
          (Thread/sleep 100)
          (.pump viewer-pipe))
        (println "Pumping thread stopped"))))


    view))
