package ed.inf.adbs.lightdb.operators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ed.inf.adbs.lightdb.DatabaseCatalog;
import ed.inf.adbs.lightdb.utils.Schema;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * The ScanOperator class represents an operator that scans a table and
 * retrieves tuples from it.
 */
public class ScanOperator extends Operator {
    private BufferedReader reader;
    private Table table;
    private List<Column> columns;

    /**
     * Constructs a ScanOperator object with the specified table name.
     *
     * @param tableName the name of the table to scan
     */
    public ScanOperator(Table table) {
        this.table = table;
        this.columns = Schema.getInstance().getColumns(this.table);

        try {
            String tableFilePath = DatabaseCatalog.getInstance().getTableLocation(this.table);
            this.reader = new BufferedReader(new FileReader(tableFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieves the next tuple from the table.
     *
     * @return the next tuple from the table, or null if there are no more tuples
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String line = this.reader.readLine();
            if (line != null) {
                return new Tuple(Arrays.asList(line.split(", ")), new ArrayList<>(this.columns));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Resets the scan operator to the beginning of the table.
     */
    @Override
    public void reset() {
        try {
            this.reader.close();
            String tableFilePath = DatabaseCatalog.getInstance().getTableLocation(this.table);
            this.reader = new BufferedReader(new FileReader(tableFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}