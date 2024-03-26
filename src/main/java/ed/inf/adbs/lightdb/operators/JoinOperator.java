package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.List;

import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator {
    private Operator leftOperator;
    private Operator rightOperator;
    private Expression where;

    public JoinOperator(Operator leftOperator, Operator rightOperator, Expression where) {
        leftOperator.reset();
        rightOperator.reset();
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        this.where = where;
    }

    private List<Tuple> matchedTuples = new ArrayList<>();
    private Tuple currentLeftTuple;

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
                ExpressionEvaluator evaluator = new ExpressionEvaluator(mergedTuple);
                boolean result = evaluator.evaluate(this.where);
                if (result) {
                    // If join condition is satisfied, add the joined tuple to the list
                    matchedTuples.add(mergedTuple);
                }
            }
            // Reset the right operator after iterating through all its tuples
            rightOperator.reset();
        }
    }
    @Override
    public void reset() {
        leftOperator.reset(); // Reset both left and right operators
        rightOperator.reset();
    }
}
