# TASK 1
## Join Operator

This README provides an overview of the `JoinOperator` class in LightDB

### QueryPlan Class

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

### Processing the WHERE Clause

1. **Extracting join condition**
   The WHERE clause is passed to the JoinOperator during its creation. This is done in the following line of code:

   ```java
   rootOperator = new JoinOperator(rootOperator, new ScanOperator(t), plainSelect.getWhere());
   ```

   Here, plainSelect.getWhere() is the WHERE clause of the SQL query. The JoinOperator is responsible for evaluating this expression during the join operation.

2. **Evaluating join condition**
   1. First we merge the tuple obtained from the left and right operators:

   ```java
   Tuple mergedTuple = currentLeftTuple.join(rightTuple);
   ```

   2. Then we use the `ExpressionEvaluator` (see #3) class to check if the merged tuple sastisfies the join condition.
   
   ```java
        ExpressionEvaluator evaluator = new ExpressionEvaluato(mergedTuple);
        boolean result = evaluator.evaluate(this.where);
    ```

   3. Finally, if the join condition is satisfied, then we add it to the list of tuples that satisfy the condition. Doing this within the loop enables us to prevent having to compute a huge cross product and then checking the conditions for each. We then reset the right operator, essentially creating a nested loop. We are basically performing theta joins.

   ```java
    if (result) {
        matchedTuples.add(mergedTuple);
    }

    ...

    rightOperator.reset();
   ```

3. **ExpressionEvaluator**
   This class is responsible to processing the `WHERE` clause. It extends `ExpressionDeParser` as per the instructions.

4. **Why the implementation does not directly use two nested while loops**
   A more concise solution could've been to compute the tuples to return within the constructor and use an iterator to return the tuples:
   ```java
    private void initMatchedTuples() {
        Tuple leftTuple, rightTuple;
        while ((leftTuple = leftOperator.getNextTuple()) != null) {
            while ((rightTuple = rightOperator.getNextTuple()) != null) {
                Tuple mergedTuple = leftTuple.join(rightTuple);
                ExpressionEvaluator evaluator = new ExpressionEvaluator(mergedTuple);
                boolean result = evaluator.evaluate(this.where);
                if (result) {
                    matchedTuples.add(mergedTuple);
                }
            }
            rightOperator.reset();
        }
    }
   ```

    This approach will consume more memory as all the matched tuples are stored in memory at once. If the number of matched tuples is very large, we might run out of memory.

    The approach I used is more memory-efficient because it only stores the matched tuples for the current left tuple in memory at a time. 

    Here's how it works:

      1. The `getNextTuple` method starts by checking if there are any matched tuples from the previous iteration of the outer loop (i.e., the previous left tuple). If there are, it returns them one by one until there are no more matched tuples.

      2. If there are no more matched tuples, it gets the next tuple from the left operator. If there are no more left tuples, it returns null, indicating that there are no more tuples to process.

      3. It then enters a nested loop where it iterates over each tuple from the right operator. For each right tuple, it evaluates the join condition. If the join condition is satisfied, it adds the joined tuple to the list of matched tuples.

      4. After it has iterated over all the right tuples, it resets the right operator and goes back to the start of the `getNextTuple` method.

    This approach is memory-efficient because it only stores the matched tuples for the current left tuple in memory. Once it has returned all the matched tuples for a left tuple, it moves on to the next left tuple and discards the matched tuples from the previous left tuple. This means that the memory usage is proportional to the number of matched tuples for a single left tuple, rather than the total number of matched tuples.

    In contrast, if I were to store all the matched tuples in memory at once (as in the approach with two nested loops in an initialization function), the memory usage would be proportional to the total number of matched tuples, which could be much larger.

# TASK 2
## Optimization rules
### Impossible / unnecessary predicates
Within the queryplan, we check if the WHERE clause is invariant. This enables us to prevent impossible / unnecessary predicates

**Common Heuristics**
Avoiding cartesian products by using theta-joins rather than cross-products
Size of query evaluation is reduced as we do not need to store the entire cross-product for later filtering

Memory efficient implementation of JoinOperator (explained previously)
The implementation computes tuples 1 by 1 instead of everything at once

# Bugs/Behavior
No bugs per se but some test queries outputs were not in the same order as the expected output. Since there was no ORDER BY query in the SQL statement, I do not believe it should be a problem. This is confirmed by Question @98 on Piazza