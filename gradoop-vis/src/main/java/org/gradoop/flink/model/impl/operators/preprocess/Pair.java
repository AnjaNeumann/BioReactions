package org.gradoop.flink.model.impl.operators.preprocess;

/**
 * Created by rostam on 08.06.17.
 * A small pair class
 */
public class Pair<T1,T2> {
    T1 t1;
    T2 t2;

    public Pair(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getKey() {
        return t1;
    }

    public T2 getValue() {
        return t2;
    }
}
