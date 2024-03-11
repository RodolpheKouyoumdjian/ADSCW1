package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class ExpressionEvaluator extends ExpressionDeParser {
    private Tuple tuple;
    private Boolean result;

    public ExpressionEvaluator(Tuple tuple) {
        this.tuple = tuple;
    }

    /**
     * Evaluates the given expression.
     *
     * @param expression The expression to be evaluated.
     * @return The result of the evaluation.
     * @throws UnsupportedOperationException If the expression type is not
     *                                       supported.
     */
    public Boolean evaluate(Expression expression) {
        System.out.println("EXPRESSION: " + expression);
        if (expression == null) {
            this.result = true;
            return true;
        } else if (expression instanceof BinaryExpression) {
            expression.accept(this);
            return result;
        }
        throw new UnsupportedOperationException("Unsupported expression type: " + expression.getClass().getName());
    }

    /**
     * Evaluates an AndExpression.
     *
     * @param andExpression The AndExpression to be evaluated.
     */
    @Override
    public void visit(AndExpression andExpression) {
        Boolean left = evaluate(andExpression.getLeftExpression());
        Boolean right = evaluate(andExpression.getRightExpression());
        result = left && right;
    }

    /**
     * Evaluates an EqualsTo expression.
     *
     * @param equalsTo The EqualsTo expression to be evaluated.
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        Long left = handleOtherDataTypes(equalsTo.getLeftExpression());
        Long right = handleOtherDataTypes(equalsTo.getRightExpression());

        System.out.println("equalsto: " + left + "==" + right);

        result = left.equals(right);
    }

    /**
     * Evaluates a NotEqualsTo expression.
     *
     * @param notEqualsTo The NotEqualsTo expression to be evaluated.
     */
    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        Long left = handleOtherDataTypes(notEqualsTo.getLeftExpression());
        Long right = handleOtherDataTypes(notEqualsTo.getRightExpression());
        result = !left.equals(right);
    }

    /**
     * Evaluates a GreaterThan expression.
     *
     * @param greaterThan The GreaterThan expression to be evaluated.
     */
    @Override
    public void visit(GreaterThan greaterThan) {
        Long left = handleOtherDataTypes(greaterThan.getLeftExpression());
        Long right = handleOtherDataTypes(greaterThan.getRightExpression());
        result = left > right;
    }

    /**
     * Evaluates a GreaterThanEquals expression.
     *
     * @param greaterThanEquals The GreaterThanEquals expression to be evaluated.
     */
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Long left = handleOtherDataTypes(greaterThanEquals.getLeftExpression());
        Long right = handleOtherDataTypes(greaterThanEquals.getRightExpression());
        result = left >= right;
    }

    /**
     * Evaluates a MinorThan expression.
     *
     * @param minorThan The MinorThan expression to be evaluated.
     */
    @Override
    public void visit(MinorThan minorThan) {
        Long left = handleOtherDataTypes(minorThan.getLeftExpression());
        Long right = handleOtherDataTypes(minorThan.getRightExpression());

        System.out.println("minorthan: " + left + "<" + right);
        result = left < right;
    }

    /**
     * Evaluates a MinorThanEquals expression.
     *
     * @param minorThanEquals The MinorThanEquals expression to be evaluated.
     */
    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        long left = handleOtherDataTypes(minorThanEquals.getLeftExpression());
        long right = handleOtherDataTypes(minorThanEquals.getRightExpression());
        result = left <= right;
    }

    private long handleOtherDataTypes(Expression expression) {
        if (expression instanceof BinaryExpression) {
            expression.accept(this);
        }

        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }

        if (expression instanceof Column) {
            Column column = (Column) expression;
            String columnName = AliasMap.resolveAlias(column.getColumnName().toLowerCase()); // Resolve alias
            column.setColumnName(columnName);
            return parseToLong(this.tuple.getValueFromColumn(column));
        }

        if (expression instanceof Multiplication) {
            Multiplication multiplication = (Multiplication) expression;
            long left = handleOtherDataTypes(multiplication.getLeftExpression());
            long right = handleOtherDataTypes(multiplication.getRightExpression());
            return left * right;
        }

        throw new UnsupportedOperationException("Unsupported expression type: " + expression.getClass().getName());

    }

    /**
     * Parses an object to a long value.
     * 
     * @param obj the object to be parsed
     * @return the long value of the object
     */
    private long parseToLong(Object obj) {
        
        try {
            if (obj instanceof Number) {
                // If the object is already a number, cast and return its long value
                return ((Number) obj).longValue();
            } else {
                // If the object is a string, attempt to parse it as a long
                return Long.parseLong(obj.toString());
            }
        } catch (NumberFormatException e) {
            // Handle the case when parsing fails
            // For example, return a default value or throw an exception
            return 0; // Default value
        }
    }

}
