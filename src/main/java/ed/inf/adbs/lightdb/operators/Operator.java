package ed.inf.adbs.lightdb.operators;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import ed.inf.adbs.lightdb.utils.Tuple;

/**
 * The Operator class represents an abstract operator in the database system.
 * It provides methods for retrieving the next tuple, resetting the operator
 * state,
 * and dumping the operator's output to a file.
 */
public abstract class Operator {
    /**
     * Retrieves the next tuple from the operator.
     *
     * @return The next tuple, or null if there are no more tuples.
     */
    public abstract Tuple getNextTuple();

    /**
     * Resets the operator to its initial state.
     * This allows for re-execution of the operator.
     */
    public abstract void reset();

    /**
     * Dumps the output of the operator to the specified output file.
     *
     * @param outputFile The path of the output file.
     */
    public void dump(String outputFile) {
        try (PrintStream out = new PrintStream(new File(outputFile))) {
            Tuple tuple;
            while ((tuple = this.getNextTuple()) != null) {
                out.println(tuple);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
