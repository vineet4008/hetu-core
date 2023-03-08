/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AssignmentUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class PushPartialAggregationProjectionsThroughJoin
        extends PushPartialAggregationThroughJoin
{
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();
    private static final Capture<ProjectNode> PROJECT_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationProjectionsThroughJoin::isSupportedAggregationNode)
            .with(source().matching(project().capturedAs(PROJECT_NODE)
                    .with(source().matching(join().capturedAs(JOIN_NODE)))));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_NODE);
        JoinNode joinNode = captures.get(JOIN_NODE);

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        // Check if Project can be pushed down through join
        // Check if aggregations can be pushed down through join
        // First push Project through Join, then apply the rule
        Assignments assignments = projectNode.getAssignments();
        Assignments.Builder leftAssignments = Assignments.builder();
        Assignments.Builder rightAssignments = Assignments.builder();
        HashSet<Symbol> leftSymbolSet = new HashSet<>(joinNode.getLeft().getOutputSymbols());
        HashSet<Symbol> rightSymbolSet = new HashSet<>(joinNode.getRight().getOutputSymbols());
        for (Map.Entry<Symbol, RowExpression> assignment : assignments.entrySet()) {
            List<Symbol> symbols = SymbolsExtractor.extractAll(assignment.getValue());
            if (symbols.size() == 0) {
                return Result.empty();
            }
            if (leftSymbolSet.containsAll(symbols)) {
                leftAssignments.put(assignment.getKey(), assignment.getValue());
            }
            else if (rightSymbolSet.containsAll(symbols)) {
                rightAssignments.put(assignment.getKey(), assignment.getValue());
            }
            else {
                return Result.empty();
            }
        }
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();
        for (Map.Entry<String, Symbol> df : joinNode.getDynamicFilters().entrySet()) {
            if (leftSymbolSet.contains(df.getValue())) {
                leftAssignments.put(df.getValue(), new VariableReferenceExpression(df.getValue().getName(), typeProvider.get(df.getValue())));
            }
            else if (rightSymbolSet.contains(df.getValue())) {
                rightAssignments.put(df.getValue(), new VariableReferenceExpression(df.getValue().getName(), typeProvider.get(df.getValue())));
            }
        }
        if (joinNode.getFilter().isPresent()) {
            List<Symbol> symbolsList = SymbolsExtractor.extractAll(joinNode.getFilter().get());
            for (Symbol symbol : symbolsList) {
                if (leftSymbolSet.contains(symbol)) {
                    leftAssignments.putAll(AssignmentUtils.identityAssignments(typeProvider, symbol));
                }
                else if (rightSymbolSet.contains(symbol)) {
                    rightAssignments.putAll(AssignmentUtils.identityAssignments(typeProvider, symbol));
                }
            }
        }
        for (JoinNode.EquiJoinClause clause : joinNode.getCriteria()) {
            if (leftSymbolSet.contains(clause.getLeft())) {
                Assignments assignments1 = AssignmentUtils.identityAssignments(typeProvider, clause.getLeft());
                leftAssignments.putAll(assignments1);

                Assignments assignments2 = AssignmentUtils.identityAssignments(typeProvider, clause.getRight());
                rightAssignments.putAll(assignments2);
            }
            else if (rightSymbolSet.contains(clause.getRight())) {
                Assignments assignments1 = AssignmentUtils.identityAssignments(typeProvider, clause.getRight());
                leftAssignments.putAll(assignments1);

                Assignments assignments2 = AssignmentUtils.identityAssignments(typeProvider, clause.getLeft());
                rightAssignments.putAll(assignments2);
            }
        }

        if (joinNode.getLeftHashSymbol().isPresent()) {
            if (leftSymbolSet.contains(joinNode.getLeftHashSymbol().get())) {
                Assignments assignments1 = AssignmentUtils.identityAssignments(typeProvider, joinNode.getLeftHashSymbol().get());
                leftAssignments.putAll(assignments1);
            }
        }
        if (joinNode.getRightHashSymbol().isPresent()) {
            if (rightSymbolSet.contains(joinNode.getRightHashSymbol().get())) {
                Assignments assignments1 = AssignmentUtils.identityAssignments(typeProvider, joinNode.getRightHashSymbol().get());
                rightAssignments.putAll(assignments1);
            }
        }

        PlanNode leftNode = joinNode.getLeft();
        Assignments build = leftAssignments.build();
        if (build.size() > 0) {
            leftNode = new ProjectNode(context.getIdAllocator().getNextId(), joinNode.getLeft(), build);
        }

        PlanNode rightNode = joinNode.getRight();
        build = rightAssignments.build();
        if (build.size() > 0) {
            rightNode = new ProjectNode(context.getIdAllocator().getNextId(), joinNode.getRight(), build);
        }
        JoinNode newJoinNode = new JoinNode(joinNode.getId(),
                joinNode.getType(),
                leftNode,
                rightNode,
                joinNode.getCriteria(),
                projectNode.getOutputSymbols(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters());
        AggregationNode newAggrNode = new AggregationNode(aggregationNode.getId(),
                newJoinNode,
                aggregationNode.getAggregations(),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedSymbols(),
                aggregationNode.getStep(),
                aggregationNode.getHashSymbol(),
                aggregationNode.getGroupIdSymbol(),
                aggregationNode.getAggregationType(),
                aggregationNode.getFinalizeSymbol());

        if (allAggregationsOn(newAggrNode.getAggregations(), newJoinNode.getLeft().getOutputSymbols())) {
            return pushPartialToLeftChild(newAggrNode, newJoinNode, context);
        }
        if (allAggregationsOn(newAggrNode.getAggregations(), newJoinNode.getRight().getOutputSymbols())) {
            return pushPartialToRightChild(newAggrNode, newJoinNode, context);
        }

        return Result.empty();
    }
}
