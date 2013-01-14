package graphchi_clojure;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.util.LabelAnalysis;

/**
 * @author akyrola
 *         Date: 7/15/12
 */
public class ConnectedComponents implements GraphChiProgram<Integer, Integer> {


    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        final int iteration = context.getIteration();
        final int numEdges = vertex.numEdges();
        if (iteration == 0) {
            vertex.setValue(vertex.getId());
             context.getScheduler().addTask(vertex.getId());
        }

        int curMin = vertex.getValue();
        for(int i=0; i < numEdges; i++) {
            int nbLabel = vertex.edge(i).getValue();
            if (iteration == 0) nbLabel = vertex.edge(i).getVertexId(); // Note!
            if (nbLabel < curMin) {
                curMin = nbLabel;
            }
        }

        vertex.setValue(curMin);
        int label = curMin;

        if (iteration > 0) {
            for(int i=0; i < numEdges; i++) {
               if (vertex.edge(i).getValue() > label) {
                   vertex.edge(i).setValue(label);
                   context.getScheduler().addTask(vertex.edge(i).getVertexId());
               }
            }
        } else {
            // Special case for first iteration to avoid overwriting
            for(int i=0; i < vertex.numOutEdges(); i++) {
                vertex.outEdge(i).setValue(label);
            }
        }
    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);

        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(baseFilename, nShards);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());
        engine.setEnableScheduler(true);
                engine.run(new ConnectedComponents(), 30);

        System.out.println("Ready. Going to output...");

        LabelAnalysis.computeLabels(baseFilename);

        System.out.println("Finished. See file: " + baseFilename + ".components");
    }
}
