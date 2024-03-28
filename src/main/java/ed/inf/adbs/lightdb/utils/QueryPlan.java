package ed.inf.adbs.lightdb.utils;

import java.util.List;

import ed.inf.adbs.lightdb.DatabaseCatalog;
import ed.inf.adbs.lightdb.operators.DuplicateEliminationOperator;
import ed.inf.adbs.lightdb.operators.JoinOperator;
import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import ed.inf.adbs.lightdb.operators.SortOperator;
import ed.inf.adbs.lightdb.operators.SumOperator;
import ed.inf.adbs.lightdb.operators.VoidOperator;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.Map;

public class QueryPlan {
    private Operator rootOperator;

    public QueryPlan(Statement statement) {
        Select select = (Select) statement;
        PlainSelect plainSelect = select.getPlainSelect();

        // Check if the WHERE clause is invariant
        // Prevents creating unnecessary SelectOperator if the WHERE clause is always
        // true or always false
        if (plainSelect.getWhere() != null) {
            ExpressionEvaluator whereEvaluator = new ExpressionEvaluator(null);
            Map<String, Boolean> optimizeMap = whereEvaluator.checkIfWhereIsInvariant(plainSelect.getWhere());

            if (optimizeMap.get("isInvariant")) {
                if (optimizeMap.get("value")) {
                    plainSelect.setWhere(null);
                } else {
                    this.rootOperator = new VoidOperator();
                    return;
                }
            }
        }

        List<Join> joins = plainSelect.getJoins();

        // Extract the table name from the FROM clause
        FromItem fromItem = plainSelect.getFromItem();
        String fromItemTableName = fromItem.toString().split(" ")[0];

        // Populate aliasMap from FROM clause
        // Check if the fromItem has an alias. If it does, add it to the AliasMap.
        if (fromItem.getAlias() != null) {
            // Get the alias name from the fromItem.
            String fromItemAlias = fromItem.getAlias().getName();
            // Add the alias and the actual table name to the AliasMap.
            AliasMap.addAlias(fromItemAlias, fromItemTableName);
        }

        // Add the table to the DatabaseCatalog
        DatabaseCatalog.getInstance().addTable(fromItemTableName);

        // If there are joins in the query, check each join for aliases.
        if (joins != null) {
            // Iterate over each join in the query.
            for (Join join : joins) {
                // Get the FromItem of the join.
                FromItem joinFromItem = join.getFromItem();
                // Get the alias of the join FromItem.
                Alias joinAlias = joinFromItem.getAlias();
                // If the join FromItem has an alias, add it to the AliasMap.
                if (joinAlias != null) {
                    // Get the actual table name from the join FromItem.
                    String joinItemActualName = joinFromItem.toString().split(" ")[0];
                    // Get the alias name from the join FromItem.
                    String joinItemAlias = joinAlias.getName();
                    // Add the alias and the actual table name to the AliasMap.
                    AliasMap.addAlias(joinItemAlias, joinItemActualName);
                }
            }
        }

        // Create a ScanOperator for the table
        Table t = new Table(fromItemTableName).withAlias(fromItem.getAlias());
        ScanOperator scanOperator = new ScanOperator(t);
        this.rootOperator = scanOperator;

        // If there are joins, create JoinOperators
        if (joins != null) {
            for (Join join : joins) {
                String joinTableName = join.getRightItem().toString().split(" ")[0];
                DatabaseCatalog.getInstance().addTable(joinTableName);
                t = new Table(joinTableName).withAlias(join.getRightItem().getAlias());

                // Here, a new JoinOperator is created for each join in the query.
                // The current rootOperator (which could be a ScanOperator for the first join,
                // or another JoinOperator for subsequent joins)
                // is set as the left child of the new JoinOperator, and a new ScanOperator for
                // the join table is set as the right child.
                // The rootOperator is then updated to be this new JoinOperator.
                // This process effectively creates a left-deep join tree because each new join
                // is always added to the right of the current tree,
                // making the existing tree the left child of the new join.
                rootOperator = new JoinOperator(this.rootOperator, new ScanOperator(t),
                        plainSelect.getWhere());
            }
        } else {
            // Create SelectOperator if WHERE clause exists, else use the ScanOperator
            if (plainSelect.getWhere() != null) {
                this.rootOperator = new SelectOperator(scanOperator, plainSelect);
            }
        }

        // If GROUP BY clause exists, create a GroupByOperator
        // Print in pink if group by is null
        // If not all columns are selected, create a ProjectOperator
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        boolean containsSumAggregate = false;
        boolean selectAllColumns = false;
        for (SelectItem<?> selectItem : selectItems) {
            Expression exp = selectItem.getExpression();
            if (exp instanceof Function) {
                if (((Function) exp).getName().equals("SUM")) {
                    containsSumAggregate = true;
                    break;
                }
            } else if (exp instanceof AllColumns) {
                selectAllColumns = true;
                if (selectItems.size() > 1) {
                    throw new UnsupportedOperationException("Cannot select all columns with other columns");
                }
            }
        }
        if (!(selectAllColumns)) {
            if ((containsSumAggregate)
                    || plainSelect.getGroupBy() != null) {
                this.rootOperator = new SumOperator(
                        this.rootOperator,
                        plainSelect);
            } else {
                this.rootOperator = new ProjectOperator(this.rootOperator, plainSelect);
            }
        }

        // If DISTINCT keyword exists, create a DuplicateEliminationOperator
        if (plainSelect.getDistinct() != null) {
            this.rootOperator = new DuplicateEliminationOperator(this.rootOperator);
        }

        // If ORDER BY clause exists, create a SortOperator
        if (plainSelect.getOrderByElements() != null) {
            this.rootOperator = new SortOperator(this.rootOperator, plainSelect);
        }

    }

    public Operator getRootOperator() {
        return this.rootOperator;
    }
}