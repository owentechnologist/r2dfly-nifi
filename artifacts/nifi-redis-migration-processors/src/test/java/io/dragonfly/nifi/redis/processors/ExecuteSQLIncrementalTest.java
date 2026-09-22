package io.dragonfly.nifi.redis.processors;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecuteSQLIncrementalTest {

    @Test
    void firstRunWithNoPriorStateFetchesAllRowsAndPersistsTheLastIdAsCursor() throws Exception {
        String url = "jdbc:h2:mem:execsql_first;DB_CLOSE_DELAY=-1";
        seed(url, "CREATE TABLE orders (id VARCHAR(10), name VARCHAR(50))",
                "INSERT INTO orders VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Carol')");

        TestRunner runner = TestRunners.newTestRunner(ExecuteSQLIncremental.class);
        registerDbcp(runner, url);
        registerWriter(runner);
        runner.setProperty(ExecuteSQLIncremental.SQL_QUERY, "SELECT id, name FROM orders WHERE id > '${cursor}' ORDER BY id");
        runner.setProperty(ExecuteSQLIncremental.CURSOR_COLUMN, "ID");
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteSQLIncremental.REL_SUCCESS);
        assertEquals(1, success.size());
        success.get(0).assertContentEquals("1,Alice\n2,Bob\n3,Carol\n");
        success.get(0).assertAttributeEquals("mime.type", "text/plain");
        success.get(0).assertAttributeEquals("record.count", "3");
        assertEquals("3", runner.getStateManager().getState(Scope.CLUSTER).get("cursor.value"));
    }

    @Test
    void secondRunResumesFromThePersistedCursorAndFetchesOnlyNewRows() throws Exception {
        String url = "jdbc:h2:mem:execsql_second;DB_CLOSE_DELAY=-1";
        seed(url, "CREATE TABLE orders (id VARCHAR(10), name VARCHAR(50))",
                "INSERT INTO orders VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Carol')");

        TestRunner runner = TestRunners.newTestRunner(ExecuteSQLIncremental.class);
        registerDbcp(runner, url);
        registerWriter(runner);
        runner.setProperty(ExecuteSQLIncremental.SQL_QUERY, "SELECT id, name FROM orders WHERE id > '${cursor}' ORDER BY id");
        runner.setProperty(ExecuteSQLIncremental.CURSOR_COLUMN, "ID");
        runner.run();
        runner.clearTransferState();

        seed(url, "INSERT INTO orders VALUES ('4', 'Dan'), ('5', 'Eve')");
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteSQLIncremental.REL_SUCCESS);
        assertEquals(1, success.size());
        success.get(0).assertContentEquals("4,Dan\n5,Eve\n");
        assertEquals("5", runner.getStateManager().getState(Scope.CLUSTER).get("cursor.value"));
    }

    @Test
    void cursorColumnUnsetNeverPersistsStateAndRerunsTheSameQuery() throws Exception {
        String url = "jdbc:h2:mem:execsql_static;DB_CLOSE_DELAY=-1";
        seed(url, "CREATE TABLE orders (id VARCHAR(10), name VARCHAR(50))",
                "INSERT INTO orders VALUES ('1', 'Alice'), ('2', 'Bob')");

        TestRunner runner = TestRunners.newTestRunner(ExecuteSQLIncremental.class);
        registerDbcp(runner, url);
        registerWriter(runner);
        runner.setProperty(ExecuteSQLIncremental.SQL_QUERY, "SELECT id, name FROM orders ORDER BY id");
        runner.run();
        runner.clearTransferState();
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteSQLIncremental.REL_SUCCESS);
        assertEquals(1, success.size());
        success.get(0).assertContentEquals("1,Alice\n2,Bob\n");
        assertTrue(runner.getStateManager().getState(Scope.CLUSTER).toMap().isEmpty());
    }

    @Test
    void zeroRowsTransfersNothingAndThrowsNoException() throws Exception {
        String url = "jdbc:h2:mem:execsql_empty;DB_CLOSE_DELAY=-1";
        seed(url, "CREATE TABLE orders (id VARCHAR(10), name VARCHAR(50))",
                "INSERT INTO orders VALUES ('1', 'Alice')");

        TestRunner runner = TestRunners.newTestRunner(ExecuteSQLIncremental.class);
        registerDbcp(runner, url);
        registerWriter(runner);
        runner.setProperty(ExecuteSQLIncremental.SQL_QUERY, "SELECT id, name FROM orders WHERE 1 = 0");
        runner.run();

        assertEquals(0, runner.getFlowFilesForRelationship(ExecuteSQLIncremental.REL_SUCCESS).size());
        // onTrigger swallows SQL failures into an error log, so without this the test would pass
        // just as well if the query had blown up rather than legitimately matched nothing.
        assertEquals(List.of(), runner.getLogger().getErrorMessages());
    }

    @Test
    void anIntegerCursorColumnRoundTripsThroughStateAsAPlainNumber() throws Exception {
        String url = "jdbc:h2:mem:execsql_numeric;DB_CLOSE_DELAY=-1";
        seed(url, "CREATE TABLE orders (id INT, name VARCHAR(50))",
                "INSERT INTO orders VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

        TestRunner runner = TestRunners.newTestRunner(ExecuteSQLIncremental.class);
        registerDbcp(runner, url);
        registerWriter(runner);
        runner.setProperty(ExecuteSQLIncremental.SQL_QUERY,
                "SELECT id, name FROM orders WHERE id > CAST(COALESCE(NULLIF('${cursor}', ''), '0') AS INT) ORDER BY id");
        runner.setProperty(ExecuteSQLIncremental.CURSOR_COLUMN, "ID");
        runner.run();

        runner.getFlowFilesForRelationship(ExecuteSQLIncremental.REL_SUCCESS).get(0)
                .assertContentEquals("1,Alice\n2,Bob\n3,Carol\n");
        assertEquals("3", runner.getStateManager().getState(Scope.CLUSTER).get("cursor.value"));

        runner.clearTransferState();
        seed(url, "INSERT INTO orders VALUES (4, 'Dan')");
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteSQLIncremental.REL_SUCCESS);
        assertEquals(1, success.size());
        success.get(0).assertContentEquals("4,Dan\n");
    }

    private static void seed(String url, String... statements) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static void registerDbcp(TestRunner runner, String url) throws Exception {
        TestDBCPService dbcp = new TestDBCPService(url);
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteSQLIncremental.DBCP_SERVICE, "dbcp");
    }

    private static void registerWriter(TestRunner runner) throws Exception {
        MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(ExecuteSQLIncremental.RECORD_WRITER, "writer");
    }

    private static class TestDBCPService extends AbstractControllerService implements DBCPService {
        private final String url;

        TestDBCPService(String url) {
            this.url = url;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                return DriverManager.getConnection(url);
            } catch (SQLException e) {
                throw new ProcessException(e);
            }
        }
    }
}
