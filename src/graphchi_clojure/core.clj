(ns graphchi-clojure.core
  (:import [edu.cmu.graphchi GraphChiProgram]
           [edu.cmu.graphchi.engine GraphChiEngine]
           [edu.cmu.graphchi.datablocks FloatConverter]
           [edu.cmu.graphchi.util Toplist
             IdFloat]))

(set! *warn-on-reflection* true)

(defn all-in-edges-vertices [vertex]
  (map
   #(. (. vertex inEdge %) getValue)
   (range 0 (. vertex numInEdges))))

(defn all-out-edges-vertices [vertex]
  (map
   #(. (. vertex outEdge %) getValue)
   (range 0 (. vertex numOutEdges))))



(defn vertex-to-map [vertex]
  {:value (.getValue vertex)
   :in-edges (all-in-edges-vertices vertex)
   :out-edges (all-out-edges-vertices vertex)})

(defn update-in-edges [vertex in-edges]
  (map
   #(. (. vertex inEdge (int %1)) setValue %2)
   (range)
   in-edges))

(defn update-out-edges [vertex out-edges]
  (map
   #(. (. vertex outEdge (int %1)) setValue %2)
   (range)
   out-edges))

(defn update-vertex [vertex {:keys [value in-edges out-edges]}]
  (doto vertex
      (.setValue (float value))
    (update-in-edges in-edges)
    (update-out-edges out-edges)))


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

(defn create-program [update-fn]
  (proxy
      [edu.cmu.graphchi.GraphChiProgram] []
    (update [vertex context]
      (let [result (update-fn (vertex-to-map vertex) (.getIteration context))]
        (update-vertex vertex result)))
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
        (range) i)))))

(defn -main
  [& args]
  (run "../facebook/0.edges" 2 (create-program page-rank) 30))
