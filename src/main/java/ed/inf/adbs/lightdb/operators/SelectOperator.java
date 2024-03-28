package ed.inf.adbs.lightdb.operators;

import java.util.Map;

import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * The SelectOperator class represents a SELECT operation in a SQL query.
 * It extends the Operator class and overrides its methods to provide
 * the functionality of a SELECT operation. The SelectOperator acts as the root
 * and the ScanOperator as its child in the query plan. During evaluation, the
 * SelectOperator’s getNextTuple() method will grab the next tuple from its
 * child,
 * check if that tuple passes the selection condition, and if so output it.
 */
public class SelectOperator extends Operator {
    private ScanOperator operator;
    private Expression where;

    /**
     * Constructor for SelectOperator.
     * 
     * @param operator    the ScanOperator which acts as the child in the query plan
     * @param plainSelect the SELECT statement from the SQL query
     */
    public SelectOperator(ScanOperator operator, PlainSelect plainSelect) {
        this.operator = operator;
        this.where = plainSelect.getWhere();
    }

    /**
     * Retrieves the next tuple that satisfies the WHERE condition.
     * 
     * @return the next tuple that satisfies the WHERE condition, or null if no more
     *         tuples. The method will continue pulling tuples from the scan until
     *         either it finds one that passes or it receives null.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = operator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple);
            if (evaluator.evaluate(where)) {
                return tuple;
            }
        }

        return null;
    }

    /**
     * Resets the operator to its initial state.
     * This allows for re-execution of the operator.
     */
    @Override
    public void reset() {
        operator.reset();
    }
}