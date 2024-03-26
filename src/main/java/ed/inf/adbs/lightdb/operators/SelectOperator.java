package ed.inf.adbs.lightdb.operators;

import java.util.Set;
import java.util.HashSet;

import ed.inf.adbs.lightdb.utils.AliasMap;
import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
import ed.inf.adbs.lightdb.utils.TableNameExtractor;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Select;

public class SelectOperator extends Operator {
    private ScanOperator operator;
    private Expression where;

    /**
     * Constructor for SelectOperator.
     * 
     * @param operator the input operator
     * @param select   the SELECT statement
     */
    public SelectOperator(ScanOperator operator, Select select) {
        this.operator = operator;
        this.where = select.getPlainSelect().getWhere();

        // Extract table names from WHERE clause
        TableNameExtractor extractor = new TableNameExtractor();
        Set<String> whereTables = extractor.extractTableNames(this.where);

        // Extract table names from FROM clause
        Set<String> fromTables = new HashSet<>();
        FromItem fromItem = select.getPlainSelect().getFromItem();
        fromTables.add(AliasMap.resolveAlias(fromItem.getAlias().getName()));

        if (select.getPlainSelect().getJoins() != null) {
            for (Join join : select.getPlainSelect().getJoins()) {
                fromTables.add(AliasMap.resolveAlias(join.getRightItem().getAlias().getName()));
            }
        }

        // Check that all tables in WHERE clause are in FROM clause
        if (!fromTables.containsAll(whereTables)) {
            throw new IllegalArgumentException("All tables in WHERE clause must be included in FROM clause");
        }

    }

    /**
     * Retrieves the next tuple that satisfies the WHERE condition.
     * 
     * @return the next tuple that satisfies the WHERE condition, or null if no more
     *         tuples
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;

        while ((tuple = operator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple);
            boolean result = evaluator.evaluate(where);
            if (result) {
                return tuple;
            }
        }
        return null;
    }

    /**
     * Resets the operator to its initial state.
     */
    @Override
    public void reset() {
        operator.reset();
    }
}