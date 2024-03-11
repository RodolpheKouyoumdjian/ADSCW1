// package ed.inf.adbs.lightdb;

// import static org.junit.Assert.assertEquals;

// import org.junit.Test;

// import ed.inf.adbs.lightdb.operators.ScanOperator;
// import ed.inf.adbs.lightdb.operators.SelectOperator;
// import ed.inf.adbs.lightdb.utils.ExpressionEvaluator;
// import ed.inf.adbs.lightdb.utils.Schema;
// import ed.inf.adbs.lightdb.utils.Tuple;
// import net.sf.jsqlparser.expression.LongValue;
// import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
// import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
// import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
// import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
// import net.sf.jsqlparser.expression.operators.relational.MinorThan;
// import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
// import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
// import net.sf.jsqlparser.schema.Column;
// import net.sf.jsqlparser.schema.Table;

// public class ExpressionEvaluatorTest {
//     public void initSingletons() {
//         DatabaseCatalog.init("samples/db");
//         DatabaseCatalog.getInstance().addTable("Boats");
//         Schema.init("samples/db");
//     }

//     public void destroySingleTons() {
//         DatabaseCatalog.destroy();
//         Schema.destroy();
//     }

//     @Test
//     public void testEqualsTo() {
//         // This test checks if the SelectOperator correctly handles an EqualsTo
//         // condition.
//         // It sets up a condition where column D should equal column E in the "Boats"
//         // table.
//         // The expected result is a tuple where D and E are both "104".

//         initSingletons();

//         EqualsTo condition = new EqualsTo();
//         condition.setLeftExpression(new Column("D").withTable(new Table("Boats")));
//         condition.setRightExpression(new Column("E").withTable(new Table("Boats")));


//         ExpressionEvaluator evaluator = new ExpressionEvaluator(null);

//         Tuple expectedTuple = new Tuple(new String[] { "104", "104", "2" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }

//     @Test
//     public void testMultiplication() {
//         // This test checks if the SelectOperator correctly handles a Multiplication
//         // condition.
//         // It sets up a condition where column F is multiplied by column E in the
//         // "Boats" table.
//         // The expected result is a tuple with the result of the multiplication.

//         initSingletons();

//         Multiplication condition = new Multiplication();
//         condition.setLeftExpression(new Column("F").withTable(new Table("Boats")));
//         condition.setRightExpression(new Column("E").withTable(new Table("Boats")));

//         SelectOperator selectOperator = new SelectOperator(new ScanOperator("Boats"),
//                 condition);

//         Tuple expectedTuple = new Tuple(new String[] { "6" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }

//     @Test
//     public void testNotEqualsTo() {
//         // This test checks if the SelectOperator correctly handles a NotEqualsTo
//         // condition.
//         // It sets up a condition where column D should not equal column E in the
//         // "Boats" table.
//         // The expected result is a tuple where D and E are "101" and "2" respectively.

//         initSingletons();

//         NotEqualsTo condition = new NotEqualsTo();
//         condition.setLeftExpression(new Column("D").withTable(new Table("Boats")));
//         condition.setRightExpression(new Column("E").withTable(new Table("Boats")));

//         SelectOperator selectOperator = new SelectOperator(new ScanOperator("Boats"),
//                 condition);

//         Tuple expectedTuple = new Tuple(new String[] { "101", "2", "3" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }

//     @Test
//     public void testGreaterThan() {
//         // This test checks if the SelectOperator correctly handles a GreaterThan
//         // condition.
//         // It sets up a condition where column D should be greater than 101 in the
//         // "Boats" table.
//         // The expected result is a tuple where D is "102".

//         initSingletons();

//         GreaterThan condition = new GreaterThan();
//         condition.setLeftExpression(new Column("D").withTable(new Table("Boats")));
//         condition.setRightExpression(new LongValue("101"));

//         SelectOperator selectOperator = new SelectOperator(new ScanOperator("Boats"), condition);

//         Tuple expectedTuple = new Tuple(new String[] { "102", "3", "4" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }

//     @Test
//     public void testGreaterThanEquals() {
//         // This test checks if the SelectOperator correctly handles a GreaterThanEquals
//         // condition.
//         // It sets up a condition where column D should be greater than or equal to 102
//         // in the "Boats" table.
//         // The expected result is a tuple where D is "102".

//         initSingletons();

//         GreaterThanEquals condition = new GreaterThanEquals();
//         condition.setLeftExpression(new Column("D").withTable(new Table("Boats")));
//         condition.setRightExpression(new LongValue("102"));

//         SelectOperator selectOperator = new SelectOperator(new ScanOperator("Boats"),
//                 condition);

//         Tuple expectedTuple = new Tuple(new String[] { "102", "3", "4" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }

//     @Test
//     public void testMinorThan() {
//         // This test checks if the SelectOperator correctly handles a MinorThan
//         // condition.
//         // It sets up a condition where column D should be less than 102 in the "Boats"
//         // table.
//         // The expected result is a tuple where D is "101".

//         initSingletons();

//         MinorThan condition = new MinorThan();
//         condition.setLeftExpression(new Column("D").withTable(new Table("Boats")));
//         condition.setRightExpression(new LongValue("102"));

//         SelectOperator selectOperator = new SelectOperator(new ScanOperator("Boats"),
//                 condition);

//         Tuple expectedTuple = new Tuple(new String[] { "101", "2", "3" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }

//     @Test
//     public void testMinorThanEquals() {
//         // This test checks if the SelectOperator correctly handles a MinorThanEquals
//         // condition.
//         // It sets up a condition where column D should be less than or equal to 101 in
//         // the "Boats" table.
//         // The expected result is a tuple where D is "101".

//         initSingletons();

//         MinorThanEquals condition = new MinorThanEquals();
//         condition.setLeftExpression(new Column("D").withTable(new Table("Boats")));
//         condition.setRightExpression(new LongValue("101"));

//         SelectOperator selectOperator = new SelectOperator(new ScanOperator("Boats"),
//                 condition);

//         Tuple expectedTuple = new Tuple(new String[] { "101", "2", "3" });
//         Tuple actualTuple = selectOperator.getNextTuple();
//         assertEquals(expectedTuple, actualTuple);

//         destroySingleTons();
//     }
// }
