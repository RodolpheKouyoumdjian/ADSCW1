package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;

public class SortOperator extends Operator {
    private Operator operator;
    private List<OrderByElement> orderByElements;
    private List<Tuple> tuples;

    public SortOperator(Operator operator, Select select) {
        this.operator = operator;
        this.orderByElements = select.getOrderByElements();
        this.tuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = operator.getNextTuple()) != null) {
            tuples.add(tuple);
        }
        sortTuples();
    }

    private void sortTuples() {
        Collections.sort(tuples, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple t1, Tuple t2) {
                for (OrderByElement orderByElement : orderByElements) {
                    Column column = (Column) orderByElement.getExpression();
                    int comparison = t1.getValueFromColumn(column).compareTo(t2.getValueFromColumn(column));
                    if (comparison != 0) {
                        return comparison;
                    }
                }
                return 0;
            }
        });
    }

    @Override
    public Tuple getNextTuple() {
        if (!tuples.isEmpty()) {
            return tuples.remove(0);
        }
        return null;
    }

    @Override
    public void reset() {
        operator.reset();
    }

}
