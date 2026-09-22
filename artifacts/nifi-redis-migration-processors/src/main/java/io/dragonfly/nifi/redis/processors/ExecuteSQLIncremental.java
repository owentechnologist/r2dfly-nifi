package io.dragonfly.nifi.redis.processors;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A source processor (no incoming connection) that runs a user-specified SQL query on NiFi's own
 * Timer/CRON schedule and emits the result as a plain Record-shaped FlowFile via the configured
 * Record Writer - the source-side counterpart to {@link RecordToKeyRecord}, the way NiFi's bundled
 * QueryDatabaseTableRecord pairs with a downstream record reader.
 *
 * <p>When Cursor Column is set, the LAST fetched row's value for that column is persisted as the
 * next run's {@code ${cursor}} token, so SQL Query must itself {@code ORDER BY} that column
 * ascending. When unset, {@code ${cursor}} never resolves to anything meaningful and SQL Query is
 * simply rerun verbatim every trigger - a valid plain periodic snapshot.
 */
@Tags({"sql", "jdbc", "database", "source", "incremental"})
@CapabilityDescription("Runs a user-specified SQL query on NiFi's own Timer/CRON schedule and emits the "
        + "result set as a plain Record-shaped FlowFile via the configured Record Writer. Optionally tracks "
        + "an incremental cursor across runs: when Cursor Column is set, the last fetched row's value for "
        + "that column is persisted and exposed as ${cursor} on the next run (e.g. \"SELECT * FROM orders "
        + "WHERE id > ${cursor} ORDER BY id\"); when unset, SQL Query simply reruns verbatim every trigger.")
@Stateful(scopes = Scope.CLUSTER, description = "Persists the last fetched Cursor Column value under the "
        + "key 'cursor.value', resolved as ${cursor} on the next run.")
public class ExecuteSQLIncremental extends AbstractProcessor {

    static final String CURSOR_STATE_KEY = "cursor.value";

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("database-connection-pooling-service")
            .displayName("Database Connection Pooling Service")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
            .name("sql-query")
            .displayName("SQL Query")
            .description("The SQL query to run on each trigger. The token ${cursor} resolves to the "
                    + "persisted Cursor Column value from the previous run (see Cursor Column), e.g. "
                    + "\"SELECT * FROM orders WHERE id > ${cursor} ORDER BY id\".")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor CURSOR_COLUMN = new PropertyDescriptor.Builder()
            .name("cursor-column")
            .displayName("Cursor Column")
            .description("When set, the LAST fetched row's value for this column (not a computed MAX) is "
                    + "persisted as next run's ${cursor}, so SQL Query must itself ORDER BY this column "
                    + "ascending for correct incremental behavior. A generic MAX would need per-JDBC-type-"
                    + "aware comparison logic this processor deliberately does not take on - NiFi's own "
                    + "QueryDatabaseTable sidesteps the identical problem the same way, by requiring "
                    + "caller-supplied ordering semantics. When unset, ${cursor} never resolves to anything "
                    + "meaningful (empty string) and SQL Query is simply rerun verbatim every trigger - a "
                    + "valid plain periodic snapshot.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INITIAL_CURSOR_VALUE = new PropertyDescriptor.Builder()
            .name("initial-cursor-value")
            .displayName("Initial Cursor Value")
            .description("Value used as ${cursor} only on the very first run, before any cursor state is "
                    + "persisted. Falls back to an empty string if unset.")
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success").description("Each FlowFile holds the rows fetched by one SQL Query execution").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(DBCP_SERVICE, SQL_QUERY, CURSOR_COLUMN, INITIAL_CURSOR_VALUE, RECORD_WRITER);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        StateManager stateManager = context.getStateManager();
        String cursorValue;
        try {
            String persisted = stateManager.getState(Scope.CLUSTER).get(CURSOR_STATE_KEY);
            String initial = context.getProperty(INITIAL_CURSOR_VALUE).getValue();
            cursorValue = persisted != null ? persisted : (initial != null ? initial : "");
        } catch (IOException e) {
            throw new ProcessException("Unable to read persisted cursor state", e);
        }

        String sql = context.getProperty(SQL_QUERY).evaluateAttributeExpressions(Map.of("cursor", cursorValue)).getValue();
        DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        boolean cursorColumnSet = context.getProperty(CURSOR_COLUMN).isSet();
        String cursorColumnName = context.getProperty(CURSOR_COLUMN).getValue();

        FlowFile flowFile = null;
        AtomicInteger recordCount = new AtomicInteger(0);
        AtomicReference<String> lastCursorValue = new AtomicReference<>();
        AtomicReference<String> mimeType = new AtomicReference<>();

        try (Connection connection = dbcpService.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            ResultSetRecordSet recordSet = new ResultSetRecordSet(resultSet, null);
            RecordSchema writerSchema = writerFactory.getSchema(Collections.emptyMap(), recordSet.getSchema());

            flowFile = session.create();
            flowFile = session.write(flowFile, out -> {
                RecordSetWriter writer;
                try {
                    writer = writerFactory.createWriter(getLogger(), writerSchema, out, Collections.emptyMap());
                } catch (SchemaNotFoundException e) {
                    throw new ProcessException("Record Writer could not resolve a schema for the query result", e);
                }
                try (writer) {
                    writer.beginRecordSet();
                    Record record;
                    while ((record = recordSet.next()) != null) {
                        writer.write(record);
                        if (cursorColumnSet) {
                            lastCursorValue.set(String.valueOf(record.getValue(cursorColumnName)));
                        }
                    }
                    WriteResult result = writer.finishRecordSet();
                    recordCount.set(result.getRecordCount());
                    mimeType.set(writer.getMimeType());
                }
            });
        } catch (Exception e) {
            getLogger().error("Failed to execute SQL Query '{}'", sql, e);
            context.yield();
            if (flowFile != null) {
                session.remove(flowFile);
            }
            return;
        }

        if (recordCount.get() == 0) {
            session.remove(flowFile);
            getLogger().debug("SQL Query returned zero rows; nothing to emit");
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType.get());
        flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(recordCount.get()));
        session.transfer(flowFile, REL_SUCCESS);

        // State is persisted only after a successful transfer: a failure between fetch and transfer
        // must not advance the cursor past what was actually delivered.
        if (cursorColumnSet) {
            try {
                stateManager.setState(Map.of(CURSOR_STATE_KEY, lastCursorValue.get()), Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Rows were fetched and transferred, but the new cursor value could not be "
                        + "persisted; the next run will re-fetch starting from the previous cursor", e);
            }
        }
    }
}
