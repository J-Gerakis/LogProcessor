import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.processor.DatabaseManager;
import org.processor.Event;

public class DatabaseManagerUnitTest {

    private static final String TEST_DB = "jdbc:hsqldb:mem:testlogdb";

    private static DatabaseManager testDatabaseManager;

    @BeforeAll
    static void beforeAll() {
        try {
            testDatabaseManager = new DatabaseManager(TEST_DB,"SA","");
            testDatabaseManager.open();
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @AfterAll
    static void afterAll() {
        testDatabaseManager.close();
    }

    @Test
    public void testDatabaseInsert() {
        try {
            Event ev1 = new Event("sssa",4,"host","type",true);
            Event ev2 = new Event("sssb",3,null,"type",false);
            Event ev3 = new Event("sssc",3,"host",null,false);
            testDatabaseManager.insertEvent(ev1);
            testDatabaseManager.insertEvent(ev2);
            testDatabaseManager.insertEvent(ev3);
            Event ev11 = testDatabaseManager.retrieveEventById(ev1.getId());
            Event ev12 = testDatabaseManager.retrieveEventById(ev2.getId());
            Event ev13 = testDatabaseManager.retrieveEventById(ev3.getId());
            Assertions.assertEquals(ev1, ev11);
            Assertions.assertEquals(ev2, ev12);
            Assertions.assertEquals(ev3, ev13);

        } catch (Exception e) {
            Assertions.fail(e);
        }

    }

}
