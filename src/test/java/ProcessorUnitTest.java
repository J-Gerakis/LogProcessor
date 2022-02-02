import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.processor.DatabaseManager;
import org.processor.Event;
import org.processor.LogEntry;
import org.processor.Processor;
import org.processor.ProcessorException;
import java.sql.SQLException;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;

public class ProcessorUnitTest {

    private final DatabaseManager databaseManager = Mockito.mock(DatabaseManager.class);
    private final Processor processor = new Processor(databaseManager);

    @Test
    public void testJSONParsingAllValues() {
        String line1 = "{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\", \"host\":\"12345\", \"timestamp\":1491377495212}";
        LogEntry logEntry1 = Assertions.assertDoesNotThrow(() -> processor.processLine(line1));

        Assertions.assertEquals("scsmbstgra", logEntry1.getId());
        Assertions.assertEquals("STARTED", logEntry1.getState());
        Assertions.assertEquals("APPLICATION_LOG", logEntry1.getType());
        Assertions.assertEquals("12345", logEntry1.getHost());
        Assertions.assertEquals(1491377495212L, logEntry1.getTimestamp());
    }

    @Test
    public void testJSONParsingEssentialValues() {
        String line2 = "{\"id\":\"scsmbstgrb\", \"state\":\"STARTED\", \"timestamp\":1491377495213}";
        LogEntry logEntry2 = Assertions.assertDoesNotThrow(() -> processor.processLine(line2));

        Assertions.assertEquals("scsmbstgrb", logEntry2.getId());
        Assertions.assertEquals("STARTED", logEntry2.getState());
        Assertions.assertEquals(1491377495213L, logEntry2.getTimestamp());
    }

    @Test
    public void testJSONParsingRejection() {
        String faultyLine1 = "{\"state\":\"STARTED\", \"timestamp\":1491377495213}";
        String faultyLine2 = "{\"id\":\"scsmbstgra\", \"timestamp\":1491377495213}";
        String faultyLine3 = "{\"id\":\"scsmbstgrb\", \"state\":\"STARTED\"}";
        Assertions.assertThrows(IllegalArgumentException.class, () -> processor.processLine(faultyLine1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> processor.processLine(faultyLine2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> processor.processLine(faultyLine3));
    }

    @Test
    public void testGoodFileProcessing() throws SQLException {
        doNothing().when(databaseManager).open();
        doNothing().when(databaseManager).close();
        doNothing().when(databaseManager).insertEvent(isA(Event.class));

        Assertions.assertDoesNotThrow(() ->
                processor.parseFile("C:\\Projects\\LogProcessor\\src\\test\\resources\\logFile.txt")
        );
    }

    @Test
    public void testBadFileProcessing() throws SQLException {
        doNothing().when(databaseManager).open();
        doNothing().when(databaseManager).close();
        doNothing().when(databaseManager).insertEvent(isA(Event.class));

        Assertions.assertDoesNotThrow(() ->
                processor.parseFile("C:\\Projects\\LogProcessor\\src\\test\\resources\\badLogFile.txt")
        );
    }

    @Test
    public void testEmptyFile() {
        Assertions.assertThrows(ProcessorException.class, () -> processor.parseFile("C:\\Projects\\inexistentFile.txt"));
    }
}
