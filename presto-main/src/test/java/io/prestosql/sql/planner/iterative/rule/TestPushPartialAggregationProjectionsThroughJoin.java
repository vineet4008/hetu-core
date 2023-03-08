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
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushPartialAggregationProjectionsThroughJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesPartialAggregationProjectionsThroughJoin()
    {
        tester().assertThat(new PushPartialAggregationProjectionsThroughJoin())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    p.symbol("LEFT_AGGR", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(p.project(Assignments.builder()
                                            .put(p.symbol("EXPR"), p.rowExpression("LEFT_AGGR * 10"))
                                            .put(p.symbol("LEFT_GROUP_BY"), p.rowExpression("LEFT_GROUP_BY"))
                                            .put(p.symbol("RIGHT_GROUP_BY"), p.rowExpression("RIGHT_GROUP_BY"))
                                            .build(),
                                    p.join(
                                            INNER,
                                            p.valuesLong(10, p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR"), p.symbol("LEFT_HASH")),
                                            p.valuesLong(100, p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI"), p.symbol("RIGHT_GROUP_BY"), p.symbol("RIGHT_HASH")),
                                            ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                            ImmutableList.of(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR"), p.symbol("RIGHT_GROUP_BY")),
                                            Optional.of(p.rowExpression("LEFT_NON_EQUI = RIGHT_NON_EQUI")),
                                            Optional.of(p.symbol("LEFT_HASH")),
                                            Optional.of(p.symbol("RIGHT_HASH")))))
                            .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(EXPR)"), ImmutableList.of(DOUBLE))
                            .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("RIGHT_GROUP_BY"))
                            .step(PARTIAL));
                })
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI = RIGHT_NON_EQUI"),
                                aggregation(
                                        singleGroupingSet("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("EXPR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        project(ImmutableMap.of(
                                                "LEFT_EQUI", PlanMatchPattern.expression("LEFT_EQUI"),
                                                "LEFT_NON_EQUI", PlanMatchPattern.expression("LEFT_NON_EQUI"),
                                                "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                                "EXPR", PlanMatchPattern.expression("LEFT_AGGR * 10"),
                                                "LEFT_HASH", PlanMatchPattern.expression("LEFT_HASH")),
                                            values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH"))),
                                project(ImmutableMap.of(
                                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                                        "RIGHT_NON_EQUI", PlanMatchPattern.expression("RIGHT_NON_EQUI"),
                                        "RIGHT_EQUI", PlanMatchPattern.expression("RIGHT_EQUI"),
                                        "RIGHT_HASH", PlanMatchPattern.expression("RIGHT_HASH")),
                                    values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH")))));
    }

    @Test
    public void testPushesPartialAggregationProjectionsThroughJoinRight()
    {
        tester().assertThat(new PushPartialAggregationProjectionsThroughJoin())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    p.symbol("RIGHT_AGGR", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(p.project(Assignments.builder()
                                            .put(p.symbol("EXPR"), p.rowExpression("RIGHT_AGGR * 10"))
                                            .put(p.symbol("LEFT_GROUP_BY"), p.rowExpression("LEFT_GROUP_BY"))
                                            .put(p.symbol("RIGHT_GROUP_BY"), p.rowExpression("RIGHT_GROUP_BY"))
                                            .build(),
                                    p.join(
                                            INNER,
                                            p.valuesLong(10, p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_HASH")),
                                            p.valuesLong(100, p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI"), p.symbol("RIGHT_GROUP_BY"), p.symbol("RIGHT_AGGR"), p.symbol("RIGHT_HASH")),
                                            ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                            ImmutableList.of(p.symbol("LEFT_GROUP_BY"), p.symbol("RIGHT_AGGR"), p.symbol("RIGHT_GROUP_BY")),
                                            Optional.of(p.rowExpression("LEFT_NON_EQUI = RIGHT_NON_EQUI")),
                                            Optional.of(p.symbol("LEFT_HASH")),
                                            Optional.of(p.symbol("RIGHT_HASH")))))
                            .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(EXPR)"), ImmutableList.of(DOUBLE))
                            .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("RIGHT_GROUP_BY"))
                            .step(PARTIAL));
                })
                .matches(project(ImmutableMap.of(
                                "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                                "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI = RIGHT_NON_EQUI"),
                                project(ImmutableMap.of(
                                                "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                                "LEFT_NON_EQUI", PlanMatchPattern.expression("LEFT_NON_EQUI"),
                                                "LEFT_EQUI", PlanMatchPattern.expression("LEFT_EQUI"),
                                                "LEFT_HASH", PlanMatchPattern.expression("LEFT_HASH")),
                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH")),
                                aggregation(
                                        singleGroupingSet("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("EXPR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        project(ImmutableMap.of(
                                                        "RIGHT_EQUI", PlanMatchPattern.expression("RIGHT_EQUI"),
                                                        "RIGHT_NON_EQUI", PlanMatchPattern.expression("RIGHT_NON_EQUI"),
                                                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                                                        "EXPR", PlanMatchPattern.expression("RIGHT_AGGR * 10"),
                                                        "RIGHT_HASH", PlanMatchPattern.expression("RIGHT_HASH")),
                                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_AGGR", "RIGHT_HASH"))))));
    }
}
