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

package com.facebook.presto.cost;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ColumnStatistics.Builder;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.facebook.presto.cost.TableScanStatsRule.toSymbolStatistics;
import static java.util.Objects.requireNonNull;

public class ConnectorFilterCalculatorService
        implements FilterStatsCalculatorService
{
    private final FilterStatsCalculator filterStatsCalculator;
    private final Metadata metadata;
    private final Random random = new Random();

    public ConnectorFilterCalculatorService(Metadata metadata, FilterStatsCalculator filterStatsCalculator)
    {
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public TableStatistics filterStats(
            TableStatistics tableStatistics,
            RowExpression predicate,
            Map<ColumnHandle, Type> columnTypes,
            ConnectorSession session)
    {
        Map<VariableReferenceExpression, ColumnHandle> storeColumnHandle = new HashMap<>();

        PlanNodeStatsEstimate beforeFilterStatsEstimate = tableStatsToPlanNodeStats(tableStatistics, columnTypes, storeColumnHandle);
        PlanNodeStatsEstimate afterFilterStatsEstimate = filterStatsCalculator.filterStats(beforeFilterStatsEstimate, predicate, session);
        return planNodeStatsToTableStats(afterFilterStatsEstimate, metadata, storeColumnHandle);
    }

    private PlanNodeStatsEstimate tableStatsToPlanNodeStats(
            TableStatistics tableStatistics,
            Map<ColumnHandle, Type> columnTypes,
            Map<VariableReferenceExpression, ColumnHandle> storeColumnHandle)
    {
        Map<VariableReferenceExpression, VariableStatsEstimate> outputVariableStats = new HashMap<>();

        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : tableStatistics.getColumnStatistics().entrySet()) {
            String tmpName = entry.getKey().toString() + random.nextInt();
            Type type = columnTypes.get(entry.getKey());
            VariableReferenceExpression variableReferenceExpression = new VariableReferenceExpression(tmpName, type);

            storeColumnHandle.put(variableReferenceExpression, entry.getKey());
            outputVariableStats.put(variableReferenceExpression, toSymbolStatistics(tableStatistics, entry.getValue()));
        }
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue())
                .addVariableStatistics(outputVariableStats)
                .build();
    }

    private TableStatistics planNodeStatsToTableStats(PlanNodeStatsEstimate planNodeStats, Metadata metadata, Map<VariableReferenceExpression, ColumnHandle> toColumnHandle)
    {
        double rowCount = planNodeStats.getOutputRowCount();
        TableStatistics.Builder builder = TableStatistics.builder();
        if (planNodeStats.isOutputRowCountUnknown()) {
            builder.setRowCount(Estimate.unknown());
            return builder.build();
        }

        builder.setRowCount(Estimate.of(rowCount));
        for (Map.Entry<VariableReferenceExpression, VariableStatsEstimate> entry : planNodeStats.getVariableStatistics().entrySet()) {
            builder.setColumnStatistics(toColumnHandle.get(entry.getKey()), toColumnStatistics(entry.getValue(), rowCount));
        }
        return builder.build();
    }

    private static ColumnStatistics toColumnStatistics(VariableStatsEstimate variableStatsEstimate, double rowCount)
    {
        if (variableStatsEstimate.isUnknown()) {
            return ColumnStatistics.empty();
        }
        double nullsFractionDouble = variableStatsEstimate.getNullsFraction();
        double nonNullRowsCount = rowCount * (1.0 - nullsFractionDouble);

        Builder builder = ColumnStatistics.builder();
        if (!Double.isNaN(nullsFractionDouble)) {
            builder.setNullsFraction(Estimate.of(nullsFractionDouble));
        }

        if (!Double.isNaN(variableStatsEstimate.getDistinctValuesCount())) {
            builder.setDistinctValuesCount(Estimate.of(variableStatsEstimate.getDistinctValuesCount()));
        }

        if (!Double.isNaN(variableStatsEstimate.getAverageRowSize())) {
            builder.setDataSize(Estimate.of(variableStatsEstimate.getAverageRowSize() * nonNullRowsCount));
        }

        if (!Double.isNaN(variableStatsEstimate.getLowValue()) && !Double.isNaN(variableStatsEstimate.getHighValue())) {
            builder.setRange(new DoubleRange(variableStatsEstimate.getLowValue(), variableStatsEstimate.getHighValue()));
        }
        return builder.build();
    }
}
