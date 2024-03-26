package ed.inf.adbs.lightdb.operators;

import java.util.Arrays;
import java.util.List;

import ed.inf.adbs.lightdb.utils.Schema;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * The ProjectOperator class is responsible for handling the projection part of
 * a SQL query: SELECT ***projection*** FROM table WHERE condition.
 * It provides methods for retrieving the next tuple that satisfies the
 * projection,
 * resetting the operator state, and dumping the operator's output to a file.
 */
public class ProjectOperator extends Operator {
    private Operator operator;
    private List<SelectItem<?>> selectItems;

    /**
     * Constructor for the ProjectOperator class.
     *
     * @param operator The child operator that provides the tuples to project.
     * @param select   The Select statement that specifies the projection.
     */
    public ProjectOperator(Operator operator, Select select) {
        this.operator = operator;
        this.selectItems = select.getPlainSelect().getSelectItems();
    }

    /**
     * Retrieves the next tuple that satisfies the projection.
     *
     * @return The next tuple that satisfies the projection, or null if there are no
     *         more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;

        while ((tuple = operator.getNextTuple()) != null) {
            Tuple resultTuple = null;
            for (SelectItem<?> item : this.selectItems) {
                Expression exp = item.getExpression();
                Column col = (Column) exp;

                int idx = Schema.getInstance().getColumnIndex(col);
                Tuple temp = new Tuple(Arrays.asList(tuple.get(idx)), Arrays.asList(tuple.getColumn(idx)));
                resultTuple = (resultTuple == null) ? temp : resultTuple.join(temp);
            }

            if (resultTuple != null) {
                return resultTuple;
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