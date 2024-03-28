package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * The SortOperator class is responsible for sorting the tuples produced by a
 * child operator based on the ORDER BY clause in a SQL query.
 */
public class SortOperator extends Operator {
    private Operator operator;
    private List<OrderByElement> orderByElements;
    private List<Tuple> tuples;

    /**
     * Constructor for the SortOperator class.
     * It takes an Operator and a Select statement as parameters.
     * The Operator is the child operator that provides the tuples to sort.
     * The Select statement is used to extract the ORDER BY clause.
     * 
     * @param operator The child operator that provides the tuples to sort.
     * @param select   The Select statement from which to extract the ORDER BY
     *                 clause.
     */
    public SortOperator(Operator operator, PlainSelect plainSelect) {
        this.operator = operator;
        this.orderByElements = plainSelect.getOrderByElements();
        this.tuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = operator.getNextTuple()) != null) {
            tuples.add(tuple);
        }

        // Sort the tuples based on the ORDER BY clause
        Collections.sort(tuples, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple t1, Tuple t2) {
                for (OrderByElement orderByElement : orderByElements) {
                    Expression exp = orderByElement.getExpression();
                    ExpressionEvaluator e1 = new ExpressionEvaluator(t1);
                    ExpressionEvaluator e2 = new ExpressionEvaluator(t2);
                    int comparison = e1.handleOtherDataTypes(exp).compareTo(e2.handleOtherDataTypes(exp));
                    if (comparison != 0) {
                        return comparison;
                    }
                }
                return 0;
            }
        });
    }

    @Override
    public Tuple getNextTuple() {
        if (!tuples.isEmpty()) {
            return tuples.remove(0);
        }
        return null;
    }

    @Override
    public void reset() {
        operator.reset();
    }

}
