package org.gradoop.flink.model.impl.operators.preprocess;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import java.util.Vector;

public class KeepPropertyAggregator extends PropertyValueAggregator {
    /**
     * Class version for serialization.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate value. No need to deserialize as it is just used for comparison.
     */
    private String aggregate;

    public KeepPropertyAggregator() {
        this("*", "all");
    }

    protected KeepPropertyAggregator(String propertyKey, String aggregatePropertyKey) {
        super(propertyKey, aggregatePropertyKey);
    }

    @Override
    protected boolean isInitialized() {
        return this.aggregate != null;
    }

    @Override
    protected void initializeAggregate(PropertyValue propertyValue) {
        this.aggregate=propertyValue.toString();
    }

    @Override
    protected void aggregateInternal(PropertyValue propertyValue) {
        System.out.println("what " + propertyValue.toString());
        this.aggregate+=","+propertyValue.toString();
    }

    @Override
    protected PropertyValue getAggregateInternal() {
        return PropertyValue.create(aggregate);
    }

    @Override
    public void resetAggregate() {
        aggregate = null;
    }
}
