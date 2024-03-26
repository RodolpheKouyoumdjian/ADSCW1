package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertEquals;
import java.io.IOException;

import org.junit.Test;

/**
 * Unit tests for LightDB.
 */
public class LightDBTest {

	// @Test
	// public void query1() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 1 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query1.sql",
	// "samples/output/query1.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query1.csv";
	// String actualOutputFilePath = "samples/output/query1.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query2() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 2 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query2.sql",
	// "samples/output/query2.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query2.csv";
	// String actualOutputFilePath = "samples/output/query2.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query3() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 3 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query3.sql",
	// "samples/output/query3.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query3.csv";
	// String actualOutputFilePath = "samples/output/query3.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query4() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 4 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query4.sql",
	// "samples/output/query4.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query4.csv";
	// String actualOutputFilePath = "samples/output/query4.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query5() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 5 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query5.sql",
	// "samples/output/query5.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query5.csv";
	// String actualOutputFilePath = "samples/output/query5.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query6() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 6 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query6.sql",
	// "samples/output/query6.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query6.csv";
	// String actualOutputFilePath = "samples/output/query6.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query7() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 7 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query7.sql",
	// "samples/output/query7.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query7.csv";
	// String actualOutputFilePath = "samples/output/query7.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query8() throws IOException {
	// String[] args = { "samples/db", "samples/input/query8.sql",
	// "samples/output/query8.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query8.csv";
	// String actualOutputFilePath = "samples/output/query8.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	@Test
	public void query9() throws IOException {
	System.out.println("\u001B[35m" + "########## QUERY 9 ##########" +
	"\u001B[0m");

	String[] args = { "samples/db", "samples/input/query9.sql",
	"samples/output/query9.csv" };
	LightDB.main(args);

	String expectedOutputFilePath = "samples/expected_output/query9.csv";
	String actualOutputFilePath = "samples/output/query9.csv";
	assertEquals(TestUtils.readFile(expectedOutputFilePath),
	TestUtils.readFile(actualOutputFilePath));
	}

	// @Test
	// public void query10() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 10 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query10.sql",
	// "samples/output/query10.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query10.csv";
	// String actualOutputFilePath = "samples/output/query10.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query11() throws IOException {
	// System.out.println("\u001B[35m" + "########## QUERY 11 ##########" +
	// "\u001B[0m");

	// String[] args = { "samples/db", "samples/input/query11.sql",
	// "samples/output/query11.csv" };
	// LightDB.main(args);

	// String expectedOutputFilePath = "samples/expected_output/query11.csv";
	// String actualOutputFilePath = "samples/output/query11.csv";
	// assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void query12() throws IOException {
	// 	System.out.println("\u001B[35m" + "########## QUERY 12 ##########" +
	// 			"\u001B[0m");

	// 	String[] args = { "samples/db", "samples/input/query12.sql",
	// 			"samples/output/query12.csv" };
	// 	LightDB.main(args);

	// 	String expectedOutputFilePath = "samples/expected_output/query12.csv";
	// 	String actualOutputFilePath = "samples/output/query12.csv";
	// 	assertEquals(TestUtils.readFile(expectedOutputFilePath),
	// 			TestUtils.readFile(actualOutputFilePath));
	// }

	// @Test
	// public void expressionEvaluatorEQUALSTO() {

	// System.out.println("\u001B[35m" + "########## expressionEvaluatorEQUALSTO
	// ##########" + "\u001B[0m");

	// String databaseDir = "samples/db";
	// DatabaseCatalog.init(databaseDir);
	// DatabaseCatalog.getInstance().addTable("Reserves");
	// Schema.init(databaseDir);

	// Tuple tuple = new Tuple(new String[] { "1", "2" });
	// ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple);

	// EqualsTo equalsTo = new EqualsTo();
	// equalsTo.setLeftExpression(new Column("G"));
	// equalsTo.setRightExpression(new LongValue("1"));
	// Boolean result = (Boolean) evaluator.evaluate(equalsTo);
	// // Print hello wold in blue
	// System.out.println("\u001B[34m" + "Result: " + result + "\u001B[0m");
	// assertTrue(result);
	// }
}
