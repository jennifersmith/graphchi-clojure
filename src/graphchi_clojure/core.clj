(ns graphchi-clojure.core
  (:import [edu.cmu.graphchi GraphChiProgram]
           [edu.cmu.graphchi.engine GraphChiEngine]
           [edu.cmu.graphchi.datablocks FloatConverter]))

(def create-program
  (proxy
      [edu.cmu.graphchi.GraphChiProgram] []
    
    (update [vertex context]
      (println vertex " " context)
      (if (zero? (. context getIteration))
        (. vertex setValue (float 1))
        
        (let [n (. vertex numInEdges)
              sum (reduce +
                          (doall
                           (map
                            #(. (. vertex inEdge %) getValue)
                            (range 0 n))))]

          (. vertex setValue (+ (* 0.85 sum) 0.15))))

      (let [outValue (/ (. vertex getValue) (. vertex numOutEdges))
            n (. vertex numOutEdges)
            ]
        (doall (map #(. (. vertex outEdge %) setValue outValue)))
        )
      )
    (beginInterval [context interval] (println "a") )
    (beginIteration [context])
    (endIteration [context])
    (endInterval [context interval])))


#_(if
            (or (zero? (mod (. vertex getId) 100000))
                (#{8737 2914} (. vertex getId)))
          
          (println (. vertex getId) " => " (. vertex getValue))
          )

(defn run [file shards program iterations]
  (let [engine (new GraphChiEngine file shards)]
    (doto engine
      (.setEdataConverter (new FloatConverter))
      (.setVertexDataConverter (new FloatConverter))
      (.setModifiesInedges false)
      (.run program iterations)) ;; apparently an important
    ;; optimisation

    
    ))


(defn -main
  "I don't do a whole lot."
  [& args]

  (run "edgesfoo/0.edges" 2 create-program 10)
  (println "hai!"))
