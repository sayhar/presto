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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.event.client.AbstractEventClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.FloatingPointColumnValidator;
import com.facebook.presto.verifier.checksum.OrderableArrayColumnValidator;
import com.facebook.presto.verifier.checksum.SimpleColumnValidator;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoResourceClient;
import com.facebook.presto.verifier.resolver.FailureResolverConfig;
import com.facebook.presto.verifier.resolver.FailureResolverManagerFactory;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.SkippedReason.MISMATCHED_QUERY_TYPE;
import static com.facebook.presto.verifier.framework.SkippedReason.SYNTAX_ERROR;
import static com.facebook.presto.verifier.framework.SkippedReason.UNSUPPORTED_QUERY_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestVerificationManager
{
    private static class MockPrestoAction
            implements PrestoAction
    {
        private final ErrorCodeSupplier errorCode;

        public MockPrestoAction(ErrorCodeSupplier errorCode)
        {
            this.errorCode = requireNonNull(errorCode, "errorCode is null");
        }

        @Override
        public QueryStats execute(Statement statement, QueryStage queryStage)
        {
            throw QueryException.forPresto(new RuntimeException(), Optional.of(errorCode), false, Optional.empty(), queryStage);
        }

        @Override
        public <R> QueryResult<R> execute(
                Statement statement,
                QueryStage queryStage,
                ResultSetConverter<R> converter)
        {
            throw QueryException.forPresto(new RuntimeException(), Optional.of(errorCode), false, Optional.empty(), queryStage);
        }
    }

    private static class MockPrestoResourceClient
            implements PrestoResourceClient
    {
        @Override
        public <V> V getJsonResponse(String path, JsonCodec<V> responseCodec)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockEventClient
            extends AbstractEventClient
    {
        private final List<VerifierQueryEvent> events = new ArrayList<>();

        @Override
        protected <T> void postEvent(T event)
        {
            checkArgument(event instanceof VerifierQueryEvent);
            this.events.add((VerifierQueryEvent) event);
        }

        public List<VerifierQueryEvent> getEvents()
        {
            return events;
        }
    }

    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final QualifiedName TABLE_PREFIX = QualifiedName.of("tmp_verifier");
    private static final SqlParser SQL_PARSER = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN, COLON));
    private static final QueryConfiguration QUERY_CONFIGURATION = new QueryConfiguration("test", "di", Optional.of("user"), Optional.empty(), Optional.empty());
    private static final SourceQuery SOURCE_QUERY = new SourceQuery(
            SUITE,
            NAME,
            "SELECT 1",
            "SELECT 2",
            QUERY_CONFIGURATION,
            QUERY_CONFIGURATION);
    private static final VerifierConfig VERIFIER_CONFIG = new VerifierConfig().setTestId("test");

    private MockEventClient eventClient;

    @BeforeMethod
    public void setup()
    {
        this.eventClient = new MockEventClient();
    }

    @Test
    public void testFailureRequeued()
    {
        VerificationManager manager = getVerificationManager(ImmutableList.of(SOURCE_QUERY), new MockPrestoAction(HIVE_PARTITION_DROPPED_DURING_QUERY), VERIFIER_CONFIG);
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 3);
    }

    @Test
    public void testFailureNotRequeued()
    {
        VerificationManager manager = getVerificationManager(ImmutableList.of(SOURCE_QUERY), new MockPrestoAction(GENERIC_INTERNAL_ERROR), VERIFIER_CONFIG);
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 1);
    }

    @Test
    public void testFailureRequeueDisabled()
    {
        VerificationManager manager = getVerificationManager(
                ImmutableList.of(SOURCE_QUERY),
                new MockPrestoAction(HIVE_PARTITION_DROPPED_DURING_QUERY),
                new VerifierConfig().setTestId("test").setVerificationResubmissionLimit(0));
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 1);
    }

    @Test
    public void testFilters()
    {
        List<SourceQuery> queries = ImmutableList.of(
                createSourceQuery("q1", "CREATE TABLE t1 (x int)", "CREATE TABLE t1 (x int)"),
                createSourceQuery("q2", "CREATE TABLE t1 (x int)", "CREATE TABLE t1 (x int)"),
                createSourceQuery("q3", "CREATE TABLE t1 (x int)", "CREATE TABLE t1 (x int)"),
                createSourceQuery("q4", "SHOW FUNCTIONS", "SHOW FUNCTIONS"),
                createSourceQuery("q5", "SELECT * FROM t1", "INSERT INTO t2 SELECT * FROM t1"),
                createSourceQuery("q6", "SELECT * FROM t1", "SELECT FROM t1"));
        VerificationManager manager = getVerificationManager(
                queries,
                new MockPrestoAction(GENERIC_INTERNAL_ERROR),
                new VerifierConfig()
                        .setTestId("test")
                        .setWhitelist("q2,q3,q4,q5,q6")
                        .setBlacklist("q2"));
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 0);

        List<VerifierQueryEvent> events = eventClient.getEvents();
        assertEquals(events.size(), 4);
        assertSkippedEvent(events.get(0), "q3", UNSUPPORTED_QUERY_TYPE);
        assertSkippedEvent(events.get(1), "q4", UNSUPPORTED_QUERY_TYPE);
        assertSkippedEvent(events.get(2), "q5", MISMATCHED_QUERY_TYPE);
        assertSkippedEvent(events.get(3), "q6", SYNTAX_ERROR);
    }

    private static SourceQuery createSourceQuery(String name, String controlQuery, String testQuery)
    {
        return new SourceQuery(SUITE, name, controlQuery, testQuery, QUERY_CONFIGURATION, QUERY_CONFIGURATION);
    }

    private static void assertSkippedEvent(VerifierQueryEvent event, String name, SkippedReason skippedReason)
    {
        assertEquals(event.getName(), name);
        assertEquals(event.getStatus(), SKIPPED.name());
        assertEquals(event.getSkippedReason(), skippedReason.name());
    }

    private VerificationManager getVerificationManager(List<SourceQuery> sourceQueries, PrestoAction prestoAction, VerifierConfig verifierConfig)
    {
        return new VerificationManager(
                () -> sourceQueries,
                new VerificationFactory(
                        SQL_PARSER,
                        (sourceQuery, verificationContext) -> prestoAction,
                        presto -> new QueryRewriter(SQL_PARSER, presto, ImmutableList.of(), ImmutableMap.of(CONTROL, TABLE_PREFIX, TEST, TABLE_PREFIX)),
                        new FailureResolverManagerFactory(ImmutableList.of(), new FailureResolverConfig().setEnabled(false)),
                        new MockPrestoResourceClient(),
                        new ChecksumValidator(new SimpleColumnValidator(), new FloatingPointColumnValidator(verifierConfig), new OrderableArrayColumnValidator()),
                        verifierConfig,
                        new TypeRegistry(),
                        new FailureResolverConfig().setEnabled(false)),
                SQL_PARSER,
                ImmutableSet.of(eventClient),
                ImmutableList.of(),
                new QueryConfigurationOverridesConfig(),
                new QueryConfigurationOverridesConfig(),
                verifierConfig);
    }
}
