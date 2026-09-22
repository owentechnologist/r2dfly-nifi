package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordFailureType;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordToKeyRecordTest {

    @Test
    void flatRecordBecomesAHashKeyedByADynamicRecordPathToken() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RecordToKeyRecord.class);
        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.STRING);
        reader.addSchemaField("name", RecordFieldType.STRING);
        reader.addRecord("42", "Widget");
        registerReader(runner, reader);

        runner.setProperty(RecordToKeyRecord.TARGET_TYPE, RecordToKeyRecord.TARGET_TYPE_HASH.getValue());
        runner.setProperty(RecordToKeyRecord.KEY_FORMAT, "rec:${id}");
        runner.setProperty("id", "/id");
        runner.enqueue(new byte[0]);
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(RecordToKeyRecord.REL_SUCCESS);
        assertEquals(1, success.size());
        KeyRecord keyRecord = RedisTypeSerializer.readJson(success.get(0).toByteArray());
        assertEquals("rec:42", keyRecord.key);
        assertEquals("hash", keyRecord.type);
        assertEquals(Map.of("id", "42", "name", "Widget"), keyRecord.value);
    }

    @Test
    void nestedRecordBecomesAJsonDocumentPreservingStructure() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RecordToKeyRecord.class);

        RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("city", RecordFieldType.STRING.getDataType()),
                new RecordField("zip", RecordFieldType.STRING.getDataType())));

        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.STRING);
        reader.addSchemaField(new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        reader.addRecord("7", new MapRecord(addressSchema, Map.of("city", "Springfield", "zip", "00000")));
        registerReader(runner, reader);

        runner.setProperty(RecordToKeyRecord.TARGET_TYPE, RecordToKeyRecord.TARGET_TYPE_JSON.getValue());
        runner.setProperty(RecordToKeyRecord.KEY_FORMAT, "doc:${id}");
        runner.setProperty("id", "/id");
        runner.enqueue(new byte[0]);
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(RecordToKeyRecord.REL_SUCCESS);
        assertEquals(1, success.size());
        KeyRecord keyRecord = RedisTypeSerializer.readJson(success.get(0).toByteArray());
        assertEquals("doc:7", keyRecord.key);
        assertEquals("json", keyRecord.type);
        assertEquals(Map.of("id", "7", "address", Map.of("city", "Springfield", "zip", "00000")), keyRecord.value);
    }

    @Test
    void aMalformedRecordRoutesTheFlowFileToFailureWithoutBlockingLaterFlowFiles() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RecordToKeyRecord.class);

        MockRecordParser badReader = new MockRecordParser();
        badReader.addSchemaField("id", RecordFieldType.STRING);
        badReader.addRecord("1");
        badReader.addRecord("2");
        badReader.failAfter(1, MockRecordFailureType.MALFORMED_RECORD_EXCEPTION);
        registerReader(runner, badReader);

        runner.setProperty(RecordToKeyRecord.TARGET_TYPE, RecordToKeyRecord.TARGET_TYPE_HASH.getValue());
        runner.setProperty(RecordToKeyRecord.KEY_FORMAT, "rec:${id}");
        runner.setProperty("id", "/id");
        runner.enqueue(new byte[0]);
        runner.run();

        assertEquals(0, runner.getFlowFilesForRelationship(RecordToKeyRecord.REL_SUCCESS).size(),
                "the record that succeeded before the malformed one must not leak out as a partial success");
        assertEquals(1, runner.getFlowFilesForRelationship(RecordToKeyRecord.REL_FAILURE).size());

        runner.clearTransferState();
        MockRecordParser goodReader = new MockRecordParser();
        goodReader.addSchemaField("id", RecordFieldType.STRING);
        goodReader.addRecord("3");
        runner.addControllerService("good-reader", goodReader);
        runner.enableControllerService(goodReader);
        runner.setProperty(RecordToKeyRecord.RECORD_READER, "good-reader");
        runner.enqueue(new byte[0]);
        runner.run();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(RecordToKeyRecord.REL_SUCCESS);
        assertEquals(1, success.size(), "a later, well-formed FlowFile must still process fine after an earlier one failed");
        KeyRecord keyRecord = RedisTypeSerializer.readJson(success.get(0).toByteArray());
        assertEquals("rec:3", keyRecord.key);
    }

    private static void registerReader(TestRunner runner, MockRecordParser reader) throws Exception {
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(RecordToKeyRecord.RECORD_READER, "reader");
    }
}
