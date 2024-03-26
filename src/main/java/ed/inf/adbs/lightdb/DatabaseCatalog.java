package ed.inf.adbs.lightdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;

/**
 * The DatabaseCatalog class represents a catalog that stores information about tables in a database.
 */
public class DatabaseCatalog {
    private static DatabaseCatalog instance = null;
    private Map<String, String> tableLocations;
    private String databaseDir;

    /**
     * Constructs a new DatabaseCatalog object with the specified database directory.
     *
     * @param databaseDir the directory where the database is located
     */
    private DatabaseCatalog(String databaseDir) {
        this.databaseDir = databaseDir;
        tableLocations = new HashMap<>();
    }

    /**
     * Initializes the DatabaseCatalog with the specified database directory.
     *
     * @param databaseDir the directory where the database is located
     * @return the initialized DatabaseCatalog instance
     * @throws IllegalStateException if the DatabaseCatalog has already been initialized
     */
    public static DatabaseCatalog init(String databaseDir) {
        if (instance != null) {
            throw new IllegalStateException(
                    "DatabaseCatalog has already been initialized. init(String databaseDir) should only be called once.");
        }
        instance = new DatabaseCatalog(databaseDir);

        return instance;
    }

    /**
     * Destroys the DatabaseCatalog instance.
     */
    public static void destroy() {
        if (instance != null) {
            instance.databaseDir = null;
            instance.tableLocations.clear();
        }
        instance = null;
    }

    /**
     * Returns the DatabaseCatalog instance.
     *
     * @return the DatabaseCatalog instance
     * @throws IllegalStateException if the DatabaseCatalog has not been initialized
     */
    public static DatabaseCatalog getInstance() {
        if (instance == null) {
            throw new IllegalStateException(
                    "DatabaseCatalog has not been initialized. Call init(String databaseDir) first.");
        }
        return instance;
    }

    /**
     * Adds a table to the catalog with the specified table name.
     *
     * @param tableName the name of the table to be added
     */
    public void addTable(String tableName) {
        tableLocations.put(tableName, databaseDir + File.separator + "data" + File.separator + tableName + ".csv");
    }

    /**
     * Returns the location of the specified table.
     *
     * @param tableName the name of the table
     * @return the location of the table
     */
    public String getTableLocation(Table table) {
        return tableLocations.get(table.getName());
    }
}