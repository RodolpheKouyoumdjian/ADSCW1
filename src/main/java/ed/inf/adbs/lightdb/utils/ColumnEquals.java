package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class ColumnEquals {
    /**
     * Updates the table of the column if the table name is an alias.
     *
     * @param column The column whose table may need to be updated.
     */
    private static void setColumnTableAlias(Column column) {
        // Check if the column table name is a key in the table map or if it is an alias
        // If it is an alias, get the actual table name and the current alias and update
        // the table
        Table table = column.getTable();
        String tableName = table.getName();
        String actualName = AliasMap.resolveAlias(tableName);

        if (tableName != actualName) {
            Alias alias = new Alias(tableName, false);
            Table t = new Table(actualName).withAlias(alias);
            column.setTable(t);
        }
    }

    /**
     * Checks if two columns are equal, optionally considering their table aliases.
     *
     * @param column1    The first column to compare.
     * @param column2    The second column to compare.
     * @param checkAlias If true, the table aliases of the columns are considered in
     *                   the comparison.
     * @return true if the columns are equal, false otherwise.
     */
    public static boolean equals(Column column1, Column column2, boolean checkAlias) {
        setColumnTableAlias(column1);
        setColumnTableAlias(column2);

        if (checkAlias) {
            return column1.getColumnName().equals(column2.getColumnName())
                    && (column1.getTable().getAlias() == null ? column2.getTable().getAlias() == null
                            : column1.getTable().getAlias().getName().equals(column2.getTable().getAlias().getName()));
        } else {
            return column1.getColumnName().equals(column2.getColumnName())
                    && column1.getTable().getName().equals(column2.getTable().getName());
        }

    }

}