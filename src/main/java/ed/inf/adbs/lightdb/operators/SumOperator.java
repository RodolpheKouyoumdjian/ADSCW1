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

/**
 * The SumOperator class is responsible for grouping tuples and calculating the
 * sum for each group.
 * It extends the Operator class and overrides its methods.
 */
public class SumOperator extends Operator {
    private Operator operator; // The child operator
    private List<SelectItem<?>> selectItems; // The list of select items
    private ListIterator<Tuple> groupIterator; // The iterator for the groups
    private List<Expression> groupByElements; // The list of group by elements

    /**
     * Constructor for the SumOperator class.
     * It takes an Operator and a PlainSelect statement as parameters.
     * The Operator is the child operator that provides the tuples.
     * The PlainSelect statement is used to extract the select items and the group
     * by elements.
     * 
     * @param operator    The child operator that provides the tuples.
     * @param plainSelect The PlainSelect statement from which to extract the select
     *                    items and the group by elements.
     */
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

    /**
     * This method initializes the group iterator.
     * It reads all tuples from the child operator, groups them by the group by
     * elements, and calculates the sum for each group if necessary.
     */
    private void initGroupIterator() {
        List<Tuple> temp = new ArrayList<>();
        this.groupIterator = new ArrayList<Tuple>().listIterator();
        // We should have a map of tuples to group mappings
        // Each key is a representant of the group
        // Each value is the actual group
        Map<Tuple, List<Tuple>> groups = new HashMap<>();
        Tuple tuple;

        // Read all tuples from the child operator
        while ((tuple = operator.getNextTuple()) != null) {
            // Hashing algorithm (each tuple creates a key for the group)
            Tuple groupKey = new Tuple(new ArrayList<>(), new ArrayList<>());

            // If there are no group by elements, we group all tuples together
            if (groupByElements == null || groupByElements.isEmpty()) {
                boolean keySetIsEmpty = groups.keySet().isEmpty();

                // The first tuple is the key
                if (keySetIsEmpty) {
                    groupKey = tuple;
                    groups.put(groupKey, Arrays.asList(tuple));
                } else {
                    groupKey = new ArrayList<>(groups.keySet()).get(0);
                    List<Tuple> group = new ArrayList<>(groups.get(groupKey));
                    group.add(tuple);
                    groups.put(groupKey, group);
                }
                // If there are group by elements, we group the tuples by the group by elements
            } else {
                // Determine the group key from the tuple and add the tuple to that group
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

        // Handles the SELECT statement parameters and calculates the sum for each group
        // if there are any SUM aggregates. If not, it would just behave like the
        // ProjectOperator
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
    }

    /**
     * Returns the net tuple in the iterator
     *
     * @return The next tuple in the iterator, or null if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        return this.groupIterator.hasNext() ? groupIterator.next() : null;
    }

    /**
     * This method resets the child operator to its initial state.
     * This allows for re-execution of the operator.
     */
    @Override
    public void reset() {
        operator.reset();
    }
}