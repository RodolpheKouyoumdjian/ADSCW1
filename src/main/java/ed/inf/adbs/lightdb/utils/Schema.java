package ed.inf.adbs.lightdb.utils;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.File;
import java.io.IOException;

/**
 * Singleton class to represent the schema of a database table.
 */
public class Schema {
    private static Schema instance; // Singleton instance
    private static Map<String, List<Column>> tables = new HashMap<>();

    /**
     * Private constructor to load the schema from a file.
     * 
     * @param schemaFile Path to the schema file
     */
    private Schema(String schemaFile) {
        try {
            // Read the first line of the schema file
            List<String> lines = Files.readAllLines(Paths.get(schemaFile));

            for (String line : lines) {
                String[] parts = line.split(" ");
                String tableName = parts[0];
                List<Column> columns = new ArrayList<>();
                for (int i = 1; i < parts.length; i++) {
                    columns.add(new Column(new Table(null, tableName), parts[i]));
                }

                tables.put(tableName, columns);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema", e);
        }
    }

    /**
     * Initialize the singleton instance with a schema file.
     * 
     * @param schemaFile Path to the schema file
     * @return The initialized singleton instance
     */
    public static Map<String, List<Column>> init(String databaseDir) {
        if (instance != null) {
            throw new IllegalStateException(
                    "Schema has already been initialized. init(String schemaFile) should only be called once.");
        }
        instance = new Schema(databaseDir + File.separator + "schema.txt");
        return tables;
    }

    public static void destroy() {
        instance = null;
    }

    /**
     * Get the singleton instance.
     * 
     * @return The singleton instance
     */
    public static Schema getInstance() {
        if (instance == null) {
            throw new IllegalStateException(
                    "Schema is not initialized. Call init(String schemaFile) before calling getInstance().");
        }
        return instance;
    }

    /**
     * Get the column names in the schema.
     * 
     * @return List of column names
     */
    public List<Column> getColumns(Table table) {
        String tableName = table.getName();
        List<Column> originalColumns = tables.get(tableName);

        // Create a new list to hold deep copies of the columns
        List<Column> copiedColumns = new ArrayList<>(originalColumns.size());

        // Populate the new list with deep copies of the columns
        for (Column column : originalColumns) {
            // Create a deep copy of the column
            Column copiedColumn = new Column(column.getTable(), column.getColumnName());
            copiedColumns.add(copiedColumn);
        }

        // Add the table attribute to the copied columns
        for (Column copiedColumn : copiedColumns) {
            copiedColumn.setTable(table);
        }

        return copiedColumns;
    }

    /**
     * Get the index of a column in the schema.
     *
     * @param columnName Name of the column
     * @return the index of the first occurrence of the column name in
     *         this list, or -1 if this list does not contain the element
     * @throws ClassCastException   if the type of the specified element
     *                              is incompatible with this list
     *                              (<a href=
     *                              "Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified element is null and this
     *                              list does not permit null elements
     *                              (<a href=
     *                              "Collection.html#optional-restrictions">optional</a>)
     */
    public int getColumnIndex(Column column) {
        String tableName = AliasMap.resolveAlias(column.getTable().getName());
        if (!tables.containsKey(tableName)) {
            throw new RuntimeException("Table " + column.getTable().getName() + " does not exist in the schema");
        }

        List<Column> columns = tables.get(tableName);

        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);


            if (ColumnEquals.equals(col, column, false)) {
                return i;
            }
        }

        return -1;
    }
}