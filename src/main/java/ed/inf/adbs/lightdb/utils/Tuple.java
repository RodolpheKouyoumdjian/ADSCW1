package ed.inf.adbs.lightdb.utils;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;

/**
 * Represents a tuple in a database table.
 */
public class Tuple {
    private List<String> values;

    // Column names of the table the tuple is a member of. If it is a merge of
    // multiple tables it'll be the merge of the column names
    private List<Column> columns;

    /**
     * Constructs a tuple with the given values.
     *
     * @param values the values of the tuple
     */
    public Tuple(List<String> values, List<Column> columns) {
        this.values = values;
        this.columns = columns;
    }

    // Constructs tuple given single value
    public Tuple(String value) {
        this.values = new ArrayList<>();
        this.values.add(value);
    }

    /**
     * Joins the given tuple with this tuple.
     *
     * @param tuple the tuple to join with
     * @return a new tuple that is the result of joining the two tuples
     */
    public Tuple join(Tuple tuple) {
        if (tuple == null) {
            return new Tuple(new ArrayList<>(this.values), new ArrayList<>(this.columns));
        }
        List<String> newValues = new ArrayList<>(this.values);
        newValues.addAll(tuple.values);

        List<Column> newColumns = new ArrayList<>(this.columns);
        newColumns.addAll(tuple.columns);

        return new Tuple(newValues, newColumns);
    }

    /**
     * Returns the values of the tuple.
     *
     * @return the values of the tuple
     */
    public List<String> getValues() {
        return new ArrayList<>(values);
    }

    /**
     * Returns a string representation of the tuple.
     *
     * @return a string representation of the tuple
     */
    @Override
    public String toString() {
        return String.join(", ", values);
    }

    /**
     * Returns the value at the specified index in the tuple.
     *
     * @param index the index of the value to retrieve
     * @return the value at the specified index
     */
    public String get(int index) {
        return values.get(index);
    }

    /**
     * Returns the number of values in the tuple.
     *
     * @return the number of values in the tuple
     */
    public int size() {
        return values.size();
    }

    public List<Column> getColumns() {
        return new ArrayList<>(columns);
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public Long getValueFromColumn(Column column) {

        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);

            if (ColumnEquals.equals(col, column, true)) {
                return Long.parseLong(values.get(i));
            }
        }
        throw new RuntimeException();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((values == null) ? 0 : values.hashCode());
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tuple other = (Tuple) obj;

        if (values == null) {
            if (other.values != null)
                return false;
        } else if (!values.equals(other.values))
            return false;
        if (columns == null) {
            if (other.columns != null)
                return false;
        } else if (!columns.equals(other.columns)) {
            for (Column column : columns) {
                if (!ColumnEquals.equals(column, column, true)) {
                    return false;
                }
            }
        }
        return true;
    }
}
