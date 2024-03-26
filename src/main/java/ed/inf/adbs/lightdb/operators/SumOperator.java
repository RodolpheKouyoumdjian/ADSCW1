package ed.inf.adbs.lightdb.operators;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;

import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.Schema;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SumOperator extends Operator {
    private Operator operator;
    private List<SelectItem<?>> selectItems;
    private Iterator<Tuple> groupIterator;
    private List<Expression> groupByElements;

    public SumOperator(Operator operator, PlainSelect plainSelect) {
        this.operator = operator;
        this.selectItems = plainSelect.getSelectItems();
        this.groupByElements = plainSelect.getGroupBy().getGroupByExpressionList();
        plainSelect.getGroupBy();
        initGroupIterator();
    }

    private void initGroupIterator() {
        Tuple tuple;
        List<Tuple> tuples = new ArrayList<>();

        while ((tuple = operator.getNextTuple()) != null) {
            Tuple resultTuple = null;
            for (SelectItem<?> item : this.selectItems) {
                Expression exp = item.getExpression();
                if (exp instanceof Column) {
                    Column col = (Column) exp;
    
                    int idx = Schema.getInstance().getColumnIndex(col);
                    Tuple temp = new Tuple(Arrays.asList(tuple.get(idx)), Arrays.asList(tuple.getColumn(idx)));
                    resultTuple = (resultTuple == null) ? temp : resultTuple.join(temp);
                } else if (exp instanceof Function && ((Function) exp).getName().equals("SUM")) {
                    Function function = (Function) exp;
                    ExpressionList parameters = function.getParameters();
                    List<Expression> expressions = parameters.getExpressions();
                    Expression sumExpression = expressions.get(0);
                    ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple);
                    Long evaluated = evaluator.handleOtherDataTypes(sumExpression);
                    Tuple temp = new Tuple(Arrays.asList(evaluated.toString()), Arrays.asList(new Column(sumExpression.toString())));
                    resultTuple = (resultTuple == null) ? temp : resultTuple.join(temp);
                }
            }
            tuples.add(resultTuple);
        }

        // Group the tuples if necessary
        if (this.groupByElements == null) {
            groupIterator = tuples.iterator();
        } else {
            Collections.sort(tuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    for (Expression groupByElement : groupByElements) {
                        Column column = (Column) groupByElement;
                        int comparison = t1.getValueFromColumn(column).compareTo(t2.getValueFromColumn(column));
                        if (comparison != 0) {
                            return comparison;
                        }
                    }
                    return 0;
                }
            });
        }
        this.groupIterator = tuples.iterator();
    }

    /**
     * Retrieves the next tuple that satisfies the projection.
     *
     * @return The next tuple that satisfies the projection, or null if there are no
     *         more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        return this.groupIterator.hasNext() ? groupIterator.next() : null;
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