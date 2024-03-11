package ed.inf.adbs.lightdb.utils;

import java.util.Arrays;

import net.sf.jsqlparser.schema.Column;

/**
 * Represents a tuple in a database table.
 */
public class Tuple {
    private String[] values;

    // Column names of the table the tuple is a member of. If it is a merge of
    // multiple tables it'll be the merge of the column names
    private Column[] columns;

    /**
     * Constructs a tuple with the given values.
     *
     * @param values the values of the tuple
     */
    public Tuple(String[] values, Column[] columns) {
        this.values = values;
        this.columns = columns;
    }

    /**
     * Computes the hash code for the tuple.
     *
     * @return the hash code value for the tuple
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(values);
        return result;
    }

    /**
     * Checks if the given object is equal to this tuple.
     *
     * @param obj the object to compare with
     * @return true if the given object is equal to this tuple, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tuple other = (Tuple) obj;
        if (!Arrays.equals(values, other.values))
            return false;
        return true;
    }

    /**
     * Joins the given tuple with this tuple.
     *
     * @param tuple the tuple to join with
     * @return a new tuple that is the result of joining the two tuples
     */
    public Tuple join(Tuple tuple) {
        if (tuple == null) {
            return this;
        }
        String[] newValues = new String[values.length + tuple.values.length];
        System.arraycopy(values, 0, newValues, 0, values.length);
        System.arraycopy(tuple.values, 0, newValues, values.length, tuple.values.length);

        Column[] newColumns = new Column[columns.length + tuple.columns.length];
        System.arraycopy(columns, 0, newColumns, 0, columns.length);
        System.arraycopy(tuple.columns, 0, newColumns, columns.length, tuple.columns.length);

        return new Tuple(newValues, newColumns);
    }

    /**
     * Returns the values of the tuple.
     *
     * @return the values of the tuple
     */
    public String[] getValues() {
        return values;
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
        return values[index];
    }

    /**
     * Returns the number of values in the tuple.
     *
     * @return the number of values in the tuple
     */
    public int size() {
        return values.length;
    }

    public Column[] getColumns() {
        return columns;
    }

    public Column getColumn(int index) {
        return columns[index];
    }

    public String getValueFromColumn(Column column) {
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];

            if (ColumnEquals.equals(col, column)) {
                return values[i];
            }
            System.out.println(col + " != " + column);
        }
        throw new RuntimeException();
    }
}
