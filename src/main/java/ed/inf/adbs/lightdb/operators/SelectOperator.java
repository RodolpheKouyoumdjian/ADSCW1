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
     * @throws IllegalArgumentException if a table in the WHERE clause is not included in the FROM clause
     */
    public SelectOperator(ScanOperator operator, PlainSelect plainSelect) {
        this.operator = operator;
        this.where = plainSelect.getWhere();

        // Extract table names from WHERE clause
        TableNameExtractor extractor = new TableNameExtractor();
        Set<String> whereTables = extractor.extractTableNames(this.where);

        // Extract table names from FROM clause
        Set<String> fromTables = new HashSet<>();
        FromItem fromItem = plainSelect.getPlainSelect().getFromItem();
        fromTables.add(AliasMap.resolveAlias(fromItem.getAlias().getName()));

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
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
     *         tuples. The method will continue pulling tuples from the scan until
     *         either it finds one that passes or it receives null.
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
     * This allows for re-execution of the operator.
     */
    @Override
    public void reset() {
        operator.reset();
    }
}