import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.processor.DatabaseManager;
import org.processor.LogEntry;
import org.processor.Processor;
import org.processor.ProcessorException;

public class ProcessorUnitTest {

    private static final String TEST_DB = "jdbc:hsqldb:mem:testlogdb";

    @Mock
    private static DatabaseManager testDBmanager;

    @BeforeAll
    static void beforeAll() {
        try {
            testDBmanager = new DatabaseManager(TEST_DB,"SA","");
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @AfterAll
    static void afterAll() {
        try {
            testDBmanager.close();
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testJSONparsing() {
        Processor processor = new Processor(testDBmanager);
        String line1 = "{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\", \"host\":\"12345\", \"timestamp\":1491377495212}";
        LogEntry logEntry1 = processor.processLine(line1);
        String line2 = "{\"id\":\"scsmbstgrb\", \"state\":\"STARTED\", \"timestamp\":1491377495213}";
        LogEntry logEntry2 = processor.processLine(line2);

        Assertions.assertEquals("scsmbstgra", logEntry1.getId());
        Assertions.assertEquals("STARTED", logEntry1.getState());
        Assertions.assertEquals("APPLICATION_LOG", logEntry1.getType());
        Assertions.assertEquals("12345", logEntry1.getHost());
        Assertions.assertEquals(1491377495212L, logEntry1.getTimestamp());

        Assertions.assertEquals("scsmbstgrb", logEntry2.getId());
        Assertions.assertEquals("STARTED", logEntry2.getState());
        Assertions.assertEquals(1491377495213L, logEntry2.getTimestamp());
    }

    @Test
    public void testJSONparsingRejection() {
        Processor processor = new Processor(testDBmanager);
        String faultyLine1 = "{\"state\":\"STARTED\", \"timestamp\":1491377495213}";
        String faultyLine2 = "{\"id\":\"scsmbstgra\", \"timestamp\":1491377495213}";
        String faultyLine3 = "{\"id\":\"scsmbstgrb\", \"state\":\"STARTED\"}";
        Assertions.assertNull(processor.processLine(faultyLine1));
        Assertions.assertNull(processor.processLine(faultyLine2));
        Assertions.assertNull(processor.processLine(faultyLine3));
    }

    @Test
    public void testEnd2EndProcessing() {
        Processor processor = new Processor(testDBmanager);
        Assertions.assertDoesNotThrow(() ->
                processor.parseFile("C:\\Projects\\LogProcessor\\src\\test\\resources\\logFile.txt")
        );
    }

    @Test
    public void testBadFileProcessing() {
        Processor processor = new Processor(testDBmanager);
        Assertions.assertThrows(ProcessorException.class, () -> processor.parseFile("C:\\Projects\\inexistentFile.txt"));
    }
}
