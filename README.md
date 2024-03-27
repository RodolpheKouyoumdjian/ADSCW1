# Join Operator

This README provides an overview of the `JoinOperator` class in LightDB

## QueryPlan Class

The `QueryPlan` class is where the join tree is constructed. When there are joins, a loop iterates over each join in the order they appear in the `joins` list (which should be the same order they appear in the SQL query). For each join, a new `JoinOperator` is created with the current `rootOperator` as the left child and a new `ScanOperator` for the join table as the right child. The `rootOperator` is then updated to be this new `JoinOperator`.

This process effectively creates a left-deep join tree because each new join is always added to the right of the current tree, making the existing tree the left child of the new join. This is consistent with the definition of a left-deep tree, where every internal node is a binary join that combines tuples from its left child and a base relation (the right child).

Lines 65-89 in QueryPlan.java
Relevant code sample (excluding comments):

```java
if (joins != null) {
    for (Join join : joins) {
        String joinTableName = join.getRightItem().toString().split(" ")[0];
        DatabaseCatalog.getInstance().addTable(joinTableName);
        t = new Table(joinTableName).withAlias(join.getRightItem().getAlias());

        rootOperator = new JoinOperator(rootOperator, new ScanOperator(t),
                plainSelect.getWhere());
    }
} else {
    if (plainSelect.getWhere() != null) {
        this.rootOperator = new SelectOperator(scanOperator, plainSelect);
    }
}
```

## Processing the WHERE Clause

1. **Extracting join condition**
   The WHERE clause is passed to the JoinOperator during its creation. This is done in the following line of code:

   `rootOperator = new JoinOperator(rootOperator, new ScanOperator(t), plainSelect.getWhere());`

   Here, plainSelect.getWhere() is the WHERE clause of the SQL query. The JoinOperator is responsible for evaluating this expression during the join operation.

2. **Evaluating join condition**
   1. First we merge the tuple obtained from the left and right operators

   `Tuple mergedTuple = currentLeftTuple.join(rightTuple);`

   2. Then we use the `ExpressionEvaluator` class to check if the merged tuple sastisfies the join condition. See #3
   ```java
        ExpressionEvaluator evaluator = new ExpressionEvaluato(mergedTuple);
        boolean result = evaluator.evaluate(this.where);
    ```

   3. Finally, if the join condition is satisfied, then we add it to the list of tuples that satisfy the condition. Doing this within the loop enables us to prevent having to compute a huge cross product and then checking the conditions for each

   ```java
    if (result) {
        matchedTuples.add(mergedTuple);
    }
   ```
3. **ExpressionEvaluator**
   This class is responsible to processing the `WHERE` clause. It extends `ExpressionDeParser` as per the instructions.
