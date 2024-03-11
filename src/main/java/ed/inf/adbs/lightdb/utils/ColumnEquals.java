package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.schema.Column;

public class ColumnEquals {
    public static boolean equals(Column column1, Column column2) {
        String tableName1 = AliasMap.resolveAlias(column1.getTable().getName().toLowerCase());
        String tableName2 = AliasMap.resolveAlias(column2.getTable().getName().toLowerCase());

        String tableAlias1 = column1.getTable().getAlias().getName().toLowerCase();
        String tableAlias2 = column2.getTable().getAlias().getName().toLowerCase();
        
        String columnName1 = column1.getColumnName().toLowerCase();
        String columnName2 = column2.getColumnName().toLowerCase();

        return tableName1.equals(tableName2) && tableAlias1.equals(tableAlias2) && columnName1.equals(columnName2);
    }
}