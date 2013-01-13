(ns graphchi-clojure.core
  (:import [edu.cmu.graphchi GraphChiProgram]
           [edu.cmu.graphchi.engine GraphChiEngine]
           [edu.cmu.graphchi.datablocks FloatConverter]))

(defn create-program [update-fn]
  (proxy
      [GraphChiProgram]
      []
    (update [vertex context] (println "hai"))
    (beginInterval [context interval] (println "a") )
    (beginIteration [context])
    (endIteration [context])
    (endInterval [context interval])))

(defn run [file shards program iterations]
  (let [engine (new GraphChiEngine file shards)]
    (doto engine
      (.setEdataConverter (new FloatConverter))
      (.setVertexDataConverter (new FloatConverter))
      (.setModifiesInedges false)
      (.run program iterations)) ;; apparently an important optimisation
    ))


(defn -main
  "I don't do a whole lot."
  [& args]

  (println "Hello, World!"))
