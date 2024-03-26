package ed.inf.adbs.lightdb.utils;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

public class TableNameExtractor extends ExpressionVisitorAdapter {
    private Set<String> tableNames = new HashSet<>();

    @Override
    public void visit(Column column) {
        String tableName = AliasMap.resolveAlias(column.getTable().getName()); // Resolve alias
        this.tableNames.add(tableName);
    }

    public Set<String> getTableNames() {
        return new HashSet<>(tableNames);
    }

    public Set<String> extractTableNames(Expression expr) {
        expr.accept(this);
        return getTableNames();
    }
}