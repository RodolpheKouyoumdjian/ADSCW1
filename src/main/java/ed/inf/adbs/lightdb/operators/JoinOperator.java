package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.List;

import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * The JoinOperator class is responsible for handling implicit joins and
 * Cartesian products in a SQL query.
 */
public class JoinOperator extends Operator {
    private Operator leftOperator;
    private Operator rightOperator;
    private Expression where;

    /**
     * Constructs a JoinOperator with the given left and right operators and join
     * condition.
     *
     * @param leftOperator  The operator for the left relation of the join.
     * @param rightOperator The operator for the right relation of the join.
     * @param where         The join condition.
     */
    public JoinOperator(Operator leftOperator, Operator rightOperator, Expression where) {
        leftOperator.reset();
        rightOperator.reset();
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        this.where = where;
    }

    private List<Tuple> matchedTuples = new ArrayList<>();
    private Tuple currentLeftTuple;

    /**
     * Returns the next tuple that satisfies the join condition.
     *
     * @return The next tuple that satisfies the join condition, or null if there
     *         are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple rightTuple;

        while (true) {
            // If there are matched tuples, return them one by one
            if (!matchedTuples.isEmpty()) {
                return matchedTuples.remove(0);
            }

            // If no more tuples from the left operator, return null
            if ((currentLeftTuple = leftOperator.getNextTuple()) == null) {
                return null;
            }

            // Iterate over each tuple from the right operator
            while ((rightTuple = rightOperator.getNextTuple()) != null) {
                // Evaluate the join condition
                Tuple mergedTuple = currentLeftTuple.join(rightTuple);

                // Extracting join conditions from the WHERE clause and evaluating them as part of the join
                ExpressionEvaluator evaluator = new ExpressionEvaluator(mergedTuple);
                boolean result = evaluator.evaluate(this.where);

                // Prevents computing cross products by checking if the join condition is satisfied
                if (result) {
                    // If join condition is satisfied, add the joined tuple to the list
                    matchedTuples.add(mergedTuple);
                }
            }
            // Reset the right operator after iterating through all its tuples
            rightOperator.reset();
        }
    }

    /**
     * Resets the state of the operator, allowing it to be used again.
     */
    @Override
    public void reset() {
        leftOperator.reset(); // Reset both left and right operators
        rightOperator.reset();
    }
}
