package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Tuple;

/**
 * The VoidOperator class is a special operator that does not produce any tuples.
 * It is used when the query does not require any tuples to be produced.
 * Example, WHERE statement always false.
 */
public class VoidOperator extends Operator {
    /**
     * Returns null because this operator does not produce any tuples.
     *
     * @return null
     */
    @Override
    public Tuple getNextTuple() {
        return null;
    }

    /**
     * Resets the operator to its initial state.
     */
    @Override
    public void reset() {
        // Nothing to reset in this operator
    }

}
