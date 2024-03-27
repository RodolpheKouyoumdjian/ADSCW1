package ed.inf.adbs.lightdb.operators;

import java.util.HashSet;
import java.util.Set;

import ed.inf.adbs.lightdb.utils.Tuple;

/**
 * The DuplicateEliminationOperator class is responsible for eliminating
 * duplicate tuples from the result.
 * It uses a HashSet to keep track of the tuples it has already seen.
 */
public class DuplicateEliminationOperator extends Operator {
    private Operator operator; // The child operator
    private Set<Tuple> seenTuples; // The set of tuples that have already been seen

    /**
     * Constructor for the DuplicateEliminationOperator class.
     * It takes an Operator as a parameter.
     * The Operator is the child operator that provides the tuples.
     * 
     * @param operator The child operator that provides the tuples.
     */
    public DuplicateEliminationOperator(Operator operator) {
        this.operator = operator;
        this.seenTuples = new HashSet<>();
    }

    /**
     * This method returns the next tuple that has not been seen before.
     * If all tuples have been seen, it returns null.
     * 
     * @return The next tuple that has not been seen before, or null if all tuples have been seen.
     */
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

    /**
     * This method resets the child operator and clears the set of seen tuples.
     */
    @Override
    public void reset() {
        operator.reset();
        seenTuples.clear();
    }
}