package ed.inf.adbs.lightdb;

import java.io.FileReader;

import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.utils.QueryPlan;
import ed.inf.adbs.lightdb.utils.Schema;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		execute(databaseDir, inputFile, outputFile);
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void execute(String databaseDir, String inputFile, String outputFile) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));

			// Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
			if (statement != null) {
				// Init DatabaseCatalog & Schema
				Schema.init(databaseDir);
				DatabaseCatalog.init(databaseDir);

				Select select = (Select) statement;

				// Construct the query plan
				QueryPlan queryPlan = new QueryPlan(select);

				// Get the root operator of the query plan
				Operator rootOperator = queryPlan.getRootOperator();

				// Dump tuples from table to file
				rootOperator.dump(outputFile);

				// Reset the scan operator
				rootOperator.reset();

				// Clean up
				DatabaseCatalog.destroy();
				Schema.destroy();
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
