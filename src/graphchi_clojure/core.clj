(ns graphchi-clojure.core
  (:import [edu.cmu.graphchi GraphChiProgram]
           [edu.cmu.graphchi.engine GraphChiEngine]
           [edu.cmu.graphchi.datablocks FloatConverter]
           [edu.cmu.graphchi.util Toplist
             IdFloat]))

(set! *warn-on-reflection* true)

(defn all-in-edges-vertices [vertex]
  (vec
   (map
    #(. (. vertex inEdge %) getValue)
    (range 0 (. vertex numInEdges))))
  )

(defn calc-new-vertex-value [vertex context]
  (if (zero? (. context getIteration))
    1.0
    (let [sum (reduce + (all-in-edges-vertices vertex))]
      (+ (* 0.85 sum) 0.15)
      )
    )
  )

(def create-program
  (proxy
      [edu.cmu.graphchi.GraphChiProgram] []
    
    (update [vertex context]      
      (let [new-vertex-val (float (calc-new-vertex-value vertex context))
            num-out-edges (int (. vertex numOutEdges))
            outValue  (/  new-vertex-val
                         num-out-edges)]
        
        (for [n (range 0 num-out-edges)]
          (. (. vertex outEdge (int n)) setValue outValue)
          )

        (. vertex setValue  new-vertex-val)
        )
      )
    
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
        (range)
        
        i)
       ))
    ))

(defn -main
  "I don't do a whole lot."
  [& args]

  (run "edgesfoo/0.edges" 2 create-program 10)
  (println "hai!"))
