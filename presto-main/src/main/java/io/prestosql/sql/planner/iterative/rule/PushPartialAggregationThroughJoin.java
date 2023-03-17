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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.AggregationStatsRule;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.SystemSessionProperties.isPushAggregationThroughJoin;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class PushPartialAggregationThroughJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));

    protected static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashSymbol().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        // TODO: leave partial aggregation above Join?
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputSymbols())) {
            return pushPartialToLeftChild(aggregationNode, joinNode, context);
        }
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputSymbols())) {
            return pushPartialToRightChild(aggregationNode, joinNode, context);
        }

        return Result.empty();
    }

    protected static boolean allAggregationsOn(Map<Symbol, Aggregation> aggregations, List<Symbol> symbols)
    {
        Set<Symbol> inputs = aggregations.values().stream()
                .map(SymbolsExtractor::extractAll)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        return symbols.containsAll(inputs);
    }

    private double getOutToInApplicabilityRatio(Context context)
    {
        return SystemSessionProperties.getPushAggregationThroughJoinOutToInRatio(context.getSession());
    }

    private double getJoinSelectivityRatio(Context context)
    {
        return SystemSessionProperties.getPushAggregationThroughJoinSelectivity(context.getSession());
    }

    private boolean isAggrNodeNotReduceOutputRows(Context context, AggregationNode pushedAggregation)
    {
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(pushedAggregation.getSource());
        PlanNodeStatsEstimate aggrNodeStats = AggregationStatsRule.groupBy(sourceStats, pushedAggregation.getGroupingKeys(), pushedAggregation.getAggregations());
        if (sourceStats.isOutputRowCountUnknown() || aggrNodeStats.isOutputRowCountUnknown()) {
            return true;
        }
        return aggrNodeStats.getOutputRowCount() / sourceStats.getOutputRowCount() > getOutToInApplicabilityRatio(context);
    }

    private boolean hasHighSelectivityForJoin(JoinNode joinNode, Context context, boolean isAggrPushedToLeft)
    {
        PlanNodeStatsEstimate childStats;
        if (isAggrPushedToLeft) {
            childStats = context.getStatsProvider().getStats(joinNode.getLeft());
        }
        else {
            childStats = context.getStatsProvider().getStats(joinNode.getRight());
        }
        PlanNodeStatsEstimate joinStats = context.getStatsProvider().getStats(joinNode);
        if (joinStats.isOutputRowCountUnknown() || childStats.isOutputRowCountUnknown()) {
            return false;
        }

        return joinStats.getOutputRowCount() / childStats.getOutputRowCount() >= getJoinSelectivityRatio(context);
    }

    protected Result pushPartialToLeftChild(AggregationNode node, JoinNode child, Context context)
    {
        Set<Symbol> joinLeftChildSymbols = ImmutableSet.copyOf(child.getLeft().getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinLeftChildSymbols, intersection(getJoinRequiredSymbols(child), joinLeftChildSymbols));
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getLeft(), groupingSet);

        // Apply only if can reduce the record count
        if (isAggrNodeNotReduceOutputRows(context, pushedAggregation)) {
            return Result.empty();
        }

        if (hasHighSelectivityForJoin(child, context, true)) {
            return Result.ofPlanNode(pushPartialToJoin(node, child, pushedAggregation, child.getRight(), context));
        }
        return Result.empty();
    }

    protected Result pushPartialToRightChild(AggregationNode node, JoinNode child, Context context)
    {
        Set<Symbol> joinRightChildSymbols = ImmutableSet.copyOf(child.getRight().getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinRightChildSymbols, intersection(getJoinRequiredSymbols(child), joinRightChildSymbols));
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getRight(), groupingSet);

        // Apply only if can reduce the record count
        if (isAggrNodeNotReduceOutputRows(context, pushedAggregation)) {
            return Result.empty();
        }

        if (hasHighSelectivityForJoin(child, context, false)) {
            return Result.ofPlanNode(pushPartialToJoin(node, child, child.getLeft(), pushedAggregation, context));
        }
        return Result.empty();
    }

    private Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        return Streams.concat(
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                node.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                node.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private List<Symbol> getPushedDownGroupingSet(AggregationNode aggregation, Set<Symbol> availableSymbols, Set<Symbol> requiredJoinSymbols)
    {
        List<Symbol> groupingSet = aggregation.getGroupingKeys();

        // keep symbols that are directly from the join's child (availableSymbols)
        List<Symbol> pushedDownGroupingSet = groupingSet.stream()
                .filter(availableSymbols::contains)
                .collect(Collectors.toList());

        // add missing required join symbols to grouping set
        Set<Symbol> existingSymbols = new HashSet<>(pushedDownGroupingSet);
        requiredJoinSymbols.stream()
                .filter(existingSymbols::add)
                .forEach(pushedDownGroupingSet::add);

        return pushedDownGroupingSet;
    }

    private AggregationNode replaceAggregationSource(
            AggregationNode aggregation,
            PlanNode source,
            List<Symbol> groupingKeys)
    {
        return new AggregationNode(
                aggregation.getId(),
                source,
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol(),
                aggregation.getAggregationType(),
                aggregation.getFinalizeSymbol());
    }

    private PlanNode pushPartialToJoin(
            AggregationNode aggregation,
            JoinNode child,
            PlanNode leftChild,
            PlanNode rightChild,
            Context context)
    {
        JoinNode joinNode = new JoinNode(
                child.getId(),
                child.getType(),
                leftChild,
                rightChild,
                child.getCriteria(),
                ImmutableList.<Symbol>builder()
                        .addAll(leftChild.getOutputSymbols())
                        .addAll(rightChild.getOutputSymbols())
                        .build(),
                child.getFilter(),
                child.getLeftHashSymbol(),
                child.getRightHashSymbol(),
                child.getDistributionType(),
                child.isSpillable(),
                child.getDynamicFilters());
        return restrictOutputs(context.getIdAllocator(),
                                joinNode,
                                ImmutableSet.copyOf(aggregation.getOutputSymbols()),
                                true,
                                context.getSymbolAllocator().getTypes()).orElse(joinNode);
    }
}
