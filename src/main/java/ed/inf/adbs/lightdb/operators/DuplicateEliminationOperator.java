package ed.inf.adbs.lightdb.operators;

import java.util.HashSet;
import java.util.Set;

import ed.inf.adbs.lightdb.utils.Tuple;

public class DuplicateEliminationOperator extends Operator {
    private Operator operator;
    private Set<Tuple> seenTuples;

    public DuplicateEliminationOperator(Operator operator) {
        this.operator = operator;
        this.seenTuples = new HashSet<>();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = operator.getNextTuple()) != null) {
            boolean contains = false;
            for (Tuple t : seenTuples) {
                if (t.equals(tuple)) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                seenTuples.add(tuple);
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        operator.reset();
        seenTuples.clear();
    }
}