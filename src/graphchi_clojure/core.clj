(ns graphchi-clojure.core
  (:import [edu.cmu.graphchi GraphChiProgram]
           [edu.cmu.graphchi.engine GraphChiEngine]
           [edu.cmu.graphchi.aggregators VertexAggregator ForeachCallback]
           [edu.cmu.graphchi.datablocks FloatConverter IntConverter]
           [edu.cmu.graphchi.util Toplist
             IdFloat LabelAnalysis]))

;;(set! *warn-on-reflection* true)

(defn all-in-edges-vertices [vertex]
  (vec
   (map
    #(. (. vertex inEdge %) getValue)
    (range 0 (. vertex numInEdges)))))

(defn all-out-edges-vertices [vertex]
  (vec
   (map
    #(. (. vertex outEdge %) getValue)
    (range 0 (. vertex numOutEdges)))))



(defn vertex-to-map [vertex]

  {:value (.getValue vertex)
   :id (.getId vertex)
   :in-edges (all-in-edges-vertices vertex)
   :out-edges (all-out-edges-vertices vertex)})

(defn update-in-edges [vertex in-edges]
  (doall
   (map
    #(. (. vertex inEdge (int %1)) setValue %2)
    (range)
    in-edges)))

(defn update-out-edges [vertex out-edges]
  (doall
   (map
    #(. (. vertex outEdge (int %1)) setValue %2)
    (range)
    out-edges))
;;  (println "HERE!!!" (all-out-edges-vertices vertex))
  )

(defn update-vertex [vertex {:keys [dont-update-in-edges value in-edges out-edges]}]
  (doto vertex
      (.setValue (int value)) ;; change to float for the other example!
    (update-out-edges out-edges))
  (when-not dont-update-in-edges
    (update-in-edges vertex in-edges)))

(defn tally-up-labels [file outfile]
  (let [result (atom {})
        foreach-callback (proxy [ ForeachCallback] []
                             (callback [vertexId vertexValue]
                                       (swap! result #(merge-with + % {vertexValue 1}) )
                                       ))]
    (VertexAggregator/foreach file (new IntConverter) foreach-callback)
    @result))

(defn calc-new-vertex-value [{:keys [in-edges] :as vertex-map} iteration]
  (if (zero? iteration)
    (assoc vertex-map :value 1.0)
    (let [sum (reduce + in-edges)]
      (assoc vertex-map :value (+ (* 0.85 sum) 0.15)))))

(defn calc-out-edges [{:keys [value out-edges] :as vertex-map}]
  (let [n-out-edges (count out-edges)]
    (if (zero? (count out-edges))
      vertex-map
      (let [
            new-value (/ value (count out-edges))]
        (assoc vertex-map :out-edges (repeat n-out-edges new-value))))))

(defn page-rank [{:keys [in-edges] :as vertex-map} iteration]
  (-> vertex-map
      (calc-new-vertex-value iteration)
      (calc-out-edges)))

(defn init-cc [{:keys [in-edges out-edges value id]}]
  {
   :value id  ;; value as ID
   :out-edges (repeat (count out-edges) id) ;; set the outbound arrows to be the same as me
   :dont-update-in-edges  true ;; dodgy
   :reschedule true ;; process me again in subsequent iter
   }
)

(defn run-cc [{:keys [in-edges out-edges value]}]
  (let [min-in-my-region (apply min value (concat in-edges out-edges))
        new-in-edges (repeat (count in-edges) min-in-my-region)
        new-out-edges (repeat (count out-edges) min-in-my-region)
        ]
    {
     :value min-in-my-region ;; minimize
     :in-edges new-in-edges
     :out-edges new-out-edges
     :edge-rescheduling {
                         :in-edges (map not= new-in-edges in-edges)
                         :out-edges (map not= new-out-edges out-edges)}}))


(defn connected-components [vertex-map iteration]
  (if (zero? iteration)
    (init-cc vertex-map)
    (run-cc vertex-map)))

(defn create-program [update-fn]
  (proxy
      [edu.cmu.graphchi.GraphChiProgram] []
    (update [vertex context]
      (let [ original (vertex-to-map vertex)
            result (update-fn original (.getIteration context))
            scheduler (.getScheduler context)]
       ;; (println "INPUT " original)
      ;;  (println "OUTPUT " result)
        (update-vertex vertex result)
      ;;  (println "AFTER UPDATE" (vertex-to-map vertex))
        (when (result :reschedule)
          (.addTask scheduler (.getId vertex)))
        (when (result :edge-rescheduling)
          (let [{:keys [in-edges out-edges]} (result :edge-rescheduling)]
            (doall
             (map
              #(when %1 (.addTask scheduler (.getVertexId (.inEdge vertex %2))))
              in-edges
              (range))
             )
            (doall
             (map
              #(when %1 (.addTask scheduler (.getVertexId (.outEdge vertex %2))))
              out-edges
              (range))
             )
            ))))
    (beginInterval [context interval] )
    (beginIteration [context])
    (endIteration [context])
    (endInterval [context interval])))

(defn run [file shards program iterations]
  (let [engine (new GraphChiEngine file shards)]
    (doto engine
      (.setEdataConverter (new FloatConverter))
      (.setVertexDataConverter (new FloatConverter))
      (.setModifiesInedges false)
      (.run program iterations)) ;; apparently an important
    ;; optimisation

    (println "Ready")
    ;; top20 = Toplist.topListFloat(baseFilename, 20);

))

(defn run-with-scheduler [file shards program iterations]
  (let [engine (new GraphChiEngine file shards)]
    (doto engine
      (.setEdataConverter (new IntConverter))
      (.setVertexDataConverter (new IntConverter))
      (.setEnableScheduler true)
      (.run program iterations))
    (println "Ready")
    ;; top20 = Toplist.topListFloat(baseFilename, 20);

))

(defn report-top-20 [file]
    (let [top20 (Toplist/topListFloat file 20)
          i (iterator-seq (. top20 iterator) )
          ]
      (doall
       (map
        (fn [a b]
          {:index a
           :vertex (. b getVertexId)
           :value (. b getValue)}
          )
        (range) i))))

(defmulti run-algo :algo )

(defmethod run-algo :pagerank [{:keys [file n-shards iterations]}]
    (do
      (run file n-shards (create-program page-rank) iterations)
      (report-top-20 file)))

(defmethod run-algo :connected-components [{:keys [file n-shards iterations]}]
  (run-with-scheduler file n-shards (create-program connected-components) iterations)
  (tally-up-labels file "notused!"))

(defn parse-args [args]
 (reduce
  #(assoc %1 (keyword (first %2)) (second %2))
  {}
  (partition 2 args)))


(defn -main
  [algo file n-shards iterations & additional]
  (run-algo
   (merge
    {:algo (keyword algo)
     :iterations (Integer/parseInt iterations)
     :n-shards (Integer/parseInt n-shards)
     :file file}
    (parse-args additional))))
