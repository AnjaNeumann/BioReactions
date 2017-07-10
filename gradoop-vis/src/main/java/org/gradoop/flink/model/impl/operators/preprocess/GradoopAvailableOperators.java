package org.gradoop.flink.model.impl.operators.preprocess;

import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.fusion.VertexFusion;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;

import java.util.HashMap;

public class GradoopAvailableOperators {
    HashMap<String, UnaryGraphToGraphOperator> unaryGraphToGraphOperatorHashMap = new HashMap<>();
    HashMap<String, BinaryGraphToGraphOperator> binaryGraphToGraphOperatorHashMap = new HashMap<>();
    HashMap<String, BinaryGraphToValueOperator> binaryGraphToValueOperatorHashMap = new HashMap<>();

    public GradoopAvailableOperators() {


        binaryGraphToGraphOperatorHashMap.put(Combination.class.getName(), new Combination());
        binaryGraphToGraphOperatorHashMap.put(Exclusion.class.getName(), new Exclusion());
        binaryGraphToGraphOperatorHashMap.put(Overlap.class.getName(), new Overlap());
        binaryGraphToGraphOperatorHashMap.put(VertexFusion.class.getName(), new VertexFusion());

        //binaryGraphToValueOperatorHashMap.put(GraphEquality.class.getName(), new GraphEquality());
    }

    public HashMap<String, UnaryGraphToGraphOperator> getAvailableUnaryGraphToGraphOperators() {
        return unaryGraphToGraphOperatorHashMap;
    }

    public HashMap<String, BinaryGraphToGraphOperator> getAvailableBinaryGraphToGraphOperators() {
        return binaryGraphToGraphOperatorHashMap;
    }

}
