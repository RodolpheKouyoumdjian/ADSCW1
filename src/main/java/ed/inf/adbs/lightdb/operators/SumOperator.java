package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.utils.CastList;
import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SumOperator extends Operator {
    private Operator operator;
    private List<SelectItem<?>> selectItems;
    private ListIterator<Tuple> groupIterator;
    private List<Expression> groupByElements;

    public SumOperator(Operator operator, PlainSelect plainSelect) {
        this.operator = operator;
        this.selectItems = plainSelect.getSelectItems();
        GroupByElement groupByElement = plainSelect.getGroupBy();
        if (groupByElement == null) {
            this.groupByElements = null;
        } else {
            this.groupByElements = CastList.castList(Expression.class,
                    plainSelect.getGroupBy().getGroupByExpressionList());
        }
        initGroupIterator();
    }

    private void initGroupIterator() {
        List<Tuple> temp = new ArrayList<>();
        this.groupIterator = new ArrayList<Tuple>().listIterator();
        // We should have a map of tuples to group mappings
        // Each key is a representant of the group
        // Each value is the actual group
        Map<Tuple, List<Tuple>> groups = new HashMap<>();
        Tuple tuple;

        while ((tuple = operator.getNextTuple()) != null) {
            Tuple groupKey = new Tuple(new ArrayList<>(), new ArrayList<>());
            if (groupByElements == null || groupByElements.isEmpty()) {
                boolean keySetIsEmpty = groups.keySet().isEmpty();
                if (keySetIsEmpty) {
                    groupKey = tuple;
                    groups.put(groupKey, Arrays.asList(tuple));
                } else {
                    groupKey = new ArrayList<>(groups.keySet()).get(0);
                    List<Tuple> group = new ArrayList<>(groups.get(groupKey));                    System.out.println("Group: " + group);
                    System.out.println("Tuple: " + tuple);
                    group.add(tuple);
                    groups.put(groupKey, group);
                }
            } else {
                for (Expression groupByElement : groupByElements) {
                    Column col = (Column) groupByElement;
                    String value = tuple.getValueFromColumn(col).toString();
                    groupKey = groupKey.join(new Tuple(Arrays.asList(value), Arrays.asList(col)));
                }
                List<Tuple> group = groups.get(groupKey);
                if (group == null) {
                    group = new ArrayList<Tuple>();
                }
                group.add(tuple);
                groups.put(groupKey, group);
            }

        }

        for (Tuple groupKey : groups.keySet()) {
            Tuple tupleToReturn = new Tuple(new ArrayList<>(), new ArrayList<>());
            List<Tuple> group = groups.get(groupKey);

            for (SelectItem<?> selectItem : this.selectItems) {
                Expression exp = selectItem.getExpression();
                if (exp instanceof Column) {
                    Column col = (Column) exp;
                    String value = group.get(0).getValueFromColumn(col).toString();
                    tupleToReturn = tupleToReturn.join(new Tuple(Arrays.asList(value), Arrays.asList(col)));
                } else if (exp instanceof Function && ((Function) exp).getName().equals("SUM")) {
                    Function function = (Function) exp;
                    List<Expression> expressions = CastList.castList(Expression.class, function.getParameters());
                    Expression sumExpression = expressions.get(0);

                    Long sum = 0L;
                    for (Tuple groupTuple : group) {
                        ExpressionEvaluator evaluator = new ExpressionEvaluator(groupTuple);
                        Long evaluated = evaluator.handleOtherDataTypes(sumExpression);
                        sum += evaluated;
                    }
                    tupleToReturn = tupleToReturn.join(new Tuple(Arrays.asList(sum.toString()),
                            Arrays.asList(new Column(sumExpression.toString()))));
                }
            }
            temp.add(tupleToReturn);

        }

        this.groupIterator = temp.listIterator();

        for (Map.Entry<Tuple, List<Tuple>> entry : groups.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
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