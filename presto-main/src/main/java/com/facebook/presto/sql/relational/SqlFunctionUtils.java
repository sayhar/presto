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
package com.facebook.presto.sql.relational;

import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlFunctionProperties;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class SqlFunctionUtils
{
    private SqlFunctionUtils() {}

    public static Expression getSqlFunctionExpression(FunctionMetadata functionMetadata, SqlInvokedScalarFunctionImplementation implementation, SqlFunctionProperties sqlFunctionProperties, List<Expression> arguments)
    {
        checkArgument(functionMetadata.getImplementationType().equals(SQL), format("Expect SQL function, get %s", functionMetadata.getImplementationType()));
        ParsingOptions parsingOptions = ParsingOptions.builder()
                .setDecimalLiteralTreatment(sqlFunctionProperties.isParseDecimalLiteralAsDouble() ? AS_DOUBLE : AS_DECIMAL)
                .build();
        Expression expression = parseSqlFunctionExpression(implementation, sqlFunctionProperties);
        return SqlFunctionArgumentBinder.bindFunctionArguments(expression, functionMetadata.getArgumentNames().get(), arguments);
    }

    public static RowExpression getSqlFunctionRowExpression(FunctionMetadata functionMetadata, SqlInvokedScalarFunctionImplementation functionImplementation, Metadata metadata, SqlFunctionProperties sqlFunctionProperties, List<RowExpression> arguments)
    {
        Expression expression = parseSqlFunctionExpression(functionImplementation, sqlFunctionProperties);

        // Allocate variables for identifiers
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        Map<Identifier, VariableReferenceExpression> variables = buildIdentifierToVariableMap(functionMetadata, expression, sqlFunctionProperties, metadata, variableAllocator);

        // Rewrite expression with allocated variables
        Expression rewritten = rewriteSqlFunctionExpressionWithVariables(expression, variables);

        // Desugar lambda capture
        Expression lambdaCaptureDesugaredExpression = LambdaCaptureDesugaringRewriter.rewrite(rewritten, variableAllocator);

        // Translate to row expression
        return SqlFunctionArgumentBinder.bindFunctionArguments(
                SqlToRowExpressionTranslator.translate(
                        lambdaCaptureDesugaredExpression,
                        getSqlFunctionExpressionTypes(metadata, sqlFunctionProperties, lambdaCaptureDesugaredExpression, variableAllocator.getTypes().allTypes()),
                        ImmutableMap.of(),
                        metadata.getFunctionManager(),
                        metadata.getTypeManager(),
                        Optional.empty(),
                        Optional.empty(),
                        sqlFunctionProperties),
                functionMetadata.getArgumentNames().get().stream()
                        .map(Identifier::new)
                        .map(variables::get)
                        .map(VariableReferenceExpression::getName)
                        .collect(toImmutableList()),
                arguments);
    }

    private static Expression parseSqlFunctionExpression(SqlInvokedScalarFunctionImplementation functionImplementation, SqlFunctionProperties sqlFunctionProperties)
    {
        ParsingOptions parsingOptions = ParsingOptions.builder()
                .setDecimalLiteralTreatment(sqlFunctionProperties.isParseDecimalLiteralAsDouble() ? AS_DOUBLE : AS_DECIMAL)
                .build();
        return new SqlParser().createExpression(functionImplementation.getImplementation(), parsingOptions);
    }

    private static Map<NodeRef<Expression>, Type> getSqlFunctionExpressionTypes(
            Metadata metadata,
            SqlFunctionProperties sqlFunctionProperties,
            Expression expression,
            Map<String, Type> argumentTypes)
    {
        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                Optional.empty(),
                sqlFunctionProperties,
                TypeProvider.copyOf(argumentTypes),
                emptyList(),
                node -> new SemanticException(NOT_SUPPORTED, node, "SQL function does not support subquery"),
                WarningCollector.NOOP,
                false);

        analyzer.analyze(
                expression,
                Scope.builder()
                        .withRelationType(
                                RelationId.anonymous(),
                                new RelationType(argumentTypes.entrySet().stream()
                                        .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue()))
                                        .collect(toImmutableList()))).build());
        return analyzer.getExpressionTypes();
    }

    private static Map<String, Type> getFunctionArgumentTypes(FunctionMetadata functionMetadata, Metadata metadata)
    {
        List<String> argumentNames = functionMetadata.getArgumentNames().get();
        List<Type> argumentTypes = functionMetadata.getArgumentTypes().stream().map(metadata::getType).collect(toImmutableList());
        checkState(argumentNames.size() == argumentTypes.size(), format("Expect argumentNames (size %d) and argumentTypes (size %d) to be of the same size", argumentNames.size(), argumentTypes.size()));
        ImmutableMap.Builder<String, Type> typeBuilder = ImmutableMap.builder();
        for (int i = 0; i < argumentNames.size(); i++) {
            typeBuilder.put(argumentNames.get(i), argumentTypes.get(i));
        }
        return typeBuilder.build();
    }

    private static Map<Identifier, VariableReferenceExpression> buildIdentifierToVariableMap(FunctionMetadata functionMetadata, Expression sqlFunction, SqlFunctionProperties sqlFunctionProperties, Metadata metadata, PlanVariableAllocator variableAllocator)
    {
        // Allocate variables for identifiers
        Map<String, Type> argumentTypes = getFunctionArgumentTypes(functionMetadata, metadata);
        Map<NodeRef<Expression>, Type> expressionTypes = getSqlFunctionExpressionTypes(metadata, sqlFunctionProperties, sqlFunction, argumentTypes);
        Map<Identifier, VariableReferenceExpression> variables = new LinkedHashMap<>();
        for (Map.Entry<NodeRef<Expression>, Type> entry : expressionTypes.entrySet()) {
            Expression node = entry.getKey().getNode();
            if (node instanceof LambdaArgumentDeclaration) {
                LambdaArgumentDeclaration lambdaArgumentDeclaration = (LambdaArgumentDeclaration) node;
                if (!variables.containsKey(lambdaArgumentDeclaration.getName())) {
                    variables.put(lambdaArgumentDeclaration.getName(), variableAllocator.newVariable(lambdaArgumentDeclaration.getName(), entry.getValue()));
                }
            }
            else if (node instanceof Identifier && argumentTypes.containsKey(((Identifier) node).getValue())) {
                // input
                if (!variables.containsKey(node)) {
                    variables.put((Identifier) node, variableAllocator.newVariable(node, entry.getValue()));
                }
            }
        }
        return variables;
    }

    private static Expression rewriteSqlFunctionExpressionWithVariables(Expression sqlFunction, Map<Identifier, VariableReferenceExpression> variableMap)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Map<Identifier, VariableReferenceExpression>>()
        {
            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Map<Identifier, VariableReferenceExpression> context, ExpressionTreeRewriter<Map<Identifier, VariableReferenceExpression>> treeRewriter)
            {
                ImmutableList.Builder<LambdaArgumentDeclaration> newArguments = ImmutableList.builder();
                for (LambdaArgumentDeclaration argument : node.getArguments()) {
                    VariableReferenceExpression variable = context.get(argument.getName());
                    newArguments.add(new LambdaArgumentDeclaration(new Identifier(variable.getName())));
                }
                return new LambdaExpression(newArguments.build(), treeRewriter.rewrite(node.getBody(), context));
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Map<Identifier, VariableReferenceExpression> context, ExpressionTreeRewriter<Map<Identifier, VariableReferenceExpression>> treeRewriter)
            {
                return new SymbolReference(context.get(node).getName());
            }
        }, sqlFunction, variableMap);
    }

    private static final class SqlFunctionArgumentBinder
    {
        private SqlFunctionArgumentBinder() {}

        public static Expression bindFunctionArguments(Expression function, List<String> argumentNames, List<Expression> argumentValues)
        {
            checkArgument(argumentNames.size() == argumentValues.size(), format("Expect same size for argumentNames (%d) and argumentValues (%d)", argumentNames.size(), argumentValues.size()));
            ImmutableMap.Builder<String, Expression> argumentBindings = ImmutableMap.builder();
            for (int i = 0; i < argumentNames.size(); i++) {
                argumentBindings.put(argumentNames.get(i), argumentValues.get(i));
            }
            return ExpressionTreeRewriter.rewriteWith(new ExpressionFunctionVisitor(argumentBindings.build()), function);
        }

        public static RowExpression bindFunctionArguments(RowExpression function, List<String> argumentNames, List<RowExpression> argumentValues)
        {
            checkArgument(argumentNames.size() == argumentValues.size(), format("Expect same size for argumentNames (%d) and argumentValues (%d)", argumentNames.size(), argumentValues.size()));
            ImmutableMap.Builder<String, RowExpression> argumentBindings = ImmutableMap.builder();
            for (int i = 0; i < argumentNames.size(); i++) {
                argumentBindings.put(argumentNames.get(i), argumentValues.get(i));
            }
            return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Map<String, RowExpression>>()
            {
                @Override
                public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Map<String, RowExpression> context, RowExpressionTreeRewriter<Map<String, RowExpression>> treeRewriter)
                {
                    if (context.containsKey(variable.getName())) {
                        return context.get(variable.getName());
                    }
                    return variable;
                }
            }, function, argumentBindings.build());
        }

        private static class ExpressionFunctionVisitor
                extends ExpressionRewriter<Void>
        {
            private final Map<String, Expression> argumentBindings;

            public ExpressionFunctionVisitor(Map<String, Expression> argumentBindings)
            {
                this.argumentBindings = requireNonNull(argumentBindings, "argumentBindings is null");
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (argumentBindings.containsKey(node.getValue())) {
                    return argumentBindings.get(node.getValue());
                }
                return node;
            }
        }
    }
}
