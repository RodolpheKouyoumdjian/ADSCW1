package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

/**
 * Unit tests for LightDB.
 */
public class LightDBTest {

	@Test
	public void query1() throws IOException {
		testQueries(1);
	}

	@Test
	public void query2() throws IOException {
		testQueries(2);
	}

	@Test
	public void query3() throws IOException {
		testQueries(3);
	}

	@Test
	public void query4() throws IOException {
		testQueries(4);
	}

	@Test
	public void query5() throws IOException {
		testQueries(5);
	}

	@Test
	public void query6() throws IOException {
		testQueries(6);
	}

	@Test
	public void query7() throws IOException {
		testQueries(7);
	}

	@Test
	public void query8() throws IOException {
		testQueries(8);
	}

	// Works but
	// output not
	// in same order,
	// it is
	// okay since we do
	// not have
	// an
	// ORDER BY clause

	@Test
	public void query9() throws IOException {
		testQueries(9);
	}

	// Works but
	// output not
	// in same order,
	// it is
	// okay since we do
	// not have
	// an
	// ORDER BY clause

	@Test
	public void query10() throws IOException {
		testQueries(10);
	}

	@Test
	public void query11() throws IOException {
		testQueries(11);
	}

	@Test
	public void query12() throws IOException {
		testQueries(12);
	}

	public void testQueries(Integer q) throws IOException {
		String[] args = { "samples/db", "samples/input/query" + q + ".sql",
				"samples/output/query" + q + ".csv" };
		LightDB.main(args);

		String expectedOutputFilePath = "samples/expected_output/query" + q + ".csv";
		String actualOutputFilePath = "samples/output/query" + q + ".csv";
		assertEquals(TestUtils.readFile(expectedOutputFilePath),
				TestUtils.readFile(actualOutputFilePath));

	}

}
