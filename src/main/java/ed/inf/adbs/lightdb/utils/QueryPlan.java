package ed.inf.adbs.lightdb.utils;

import java.util.List;

import ed.inf.adbs.lightdb.DatabaseCatalog;
import ed.inf.adbs.lightdb.operators.JoinOperator;
import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class QueryPlan {
    private Operator rootOperator;

    public QueryPlan(Statement statement) {
        Select select = (Select) statement;
        PlainSelect plainSelect = select.getPlainSelect();
        List<Join> joins = plainSelect.getJoins();

        // Extract the table name from the FROM clause
        FromItem fromItem = plainSelect.getFromItem();
        String fromItemTableName = fromItem.toString().toLowerCase().split(" ")[0];

        // Add the table to the DatabaseCatalog
        DatabaseCatalog.getInstance().addTable(fromItemTableName);

        // Populate aliasMap from FROM clause
        if (fromItem.getAlias() != null) {
            String fromItemAlias = fromItem.getAlias().getName().toLowerCase();
            AliasMap.addAlias(fromItemAlias, fromItemTableName);
        }

        if (joins != null) {
            for (Join join : joins) {
                if (join.getRightItem().getAlias() != null) {
                    String joinItemActualName = join.getRightItem().toString().toLowerCase().split(" ")[0];
                    String joinItemAlias = join.getRightItem().getAlias().getName().toLowerCase();
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
                String joinTableName = join.getRightItem().toString().toLowerCase().split(" ")[0];
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

        // If not all columns are selected, create a ProjectOperator
        if (!(plainSelect.getSelectItems().get(0).getExpression() instanceof AllColumns)) {
            this.rootOperator = new ProjectOperator(this.rootOperator, select);
        }
    }

    public Operator getRootOperator() {
        return this.rootOperator;
    }
}