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
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

public class QueryPlan {
    private Operator rootOperator;

    public QueryPlan(Statement statement) {
        Select select = (Select) statement;
        PlainSelect plainSelect = select.getPlainSelect();
        List<Join> joins = plainSelect.getJoins();

        // Extract the table name from the FROM clause
        FromItem fromItem = plainSelect.getFromItem();
        String fromItemTableName = fromItem.toString().split(" ")[0];

        // Add the table to the DatabaseCatalog
        DatabaseCatalog.getInstance().addTable(fromItemTableName);

        // Populate aliasMap from FROM clause
        if (fromItem.getAlias() != null) {
            String fromItemAlias = fromItem.getAlias().getName();
            AliasMap.addAlias(fromItemAlias, fromItemTableName);
        }

        if (joins != null) {
            for (Join join : joins) {
                FromItem joinFromItem = join.getFromItem();
                Alias joinAlias = joinFromItem.getAlias();
                if (joinAlias != null) {
                    String joinItemActualName = joinFromItem.toString().split(" ")[0];
                    String joinItemAlias = joinAlias.getName();
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
                rootOperator = new JoinOperator(rootOperator, new ScanOperator(t),
                        select.getPlainSelect().getWhere());
            }
        } else {
            // Create SelectOperator if WHERE clause exists, else use the ScanOperator
            if (plainSelect.getWhere() != null) {
                this.rootOperator = new SelectOperator(scanOperator, select);
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
                System.out.println("SUM OPERATOR CREATED");
                this.rootOperator = new SumOperator(
                        this.rootOperator,
                        plainSelect);
            } else {
                this.rootOperator = new ProjectOperator(this.rootOperator, select);
            }
        }

        // If DISTINCT keyword exists, create a DuplicateEliminationOperator
        if (plainSelect.getDistinct() != null) {
            this.rootOperator = new DuplicateEliminationOperator(this.rootOperator);
        }

        // If ORDER BY clause exists, create a SortOperator
        if (plainSelect.getOrderByElements() != null) {
            this.rootOperator = new SortOperator(this.rootOperator, select);
        }

    }

    public Operator getRootOperator() {
        return this.rootOperator;
    }
}