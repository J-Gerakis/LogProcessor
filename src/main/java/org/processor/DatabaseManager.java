package org.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Jerome Gerakis
 * Handler object for the HSQLDB connection. Has default connection information if none are provided at instantiation.
 * Will create the DB file in the working directory, and an Event Log table if none already exists.
 *
 */
public class DatabaseManager {

    public final static Logger logger = LogManager.getLogger(DatabaseManager.class);

    public static final String DEFAULT_FILE = "jdbc:hsqldb:file:serverlogdb";
    public static final String DEFAULT_USER = "SA";
    private static final String TABLE_EVENT = "event_log";

    private static final String TABLE_EVENT_CREATION = "CREATE TABLE "+TABLE_EVENT+" (" +
            "   id VARCHAR(50) NOT NULL, " +
            "   duration BIGINT NOT NULL, " +
            "   host VARCHAR(50), " +
            "   type VARCHAR(50), " +
            "   alert BIT, " +
            "   PRIMARY KEY (id) " +
            "); ";
    private static final String INDEX_CREATION = "CREATE UNIQUE INDEX log_idx ON "+TABLE_EVENT+" (id);";

    private static final String INSERT_EVENT_STATEMENT = "INSERT INTO "+TABLE_EVENT+" (id,duration,host,type,alert) " +
            "VALUES (?, ?, ?, ?, ?);";
    private static final String SELECT_EVENT = "SELECT * FROM "+TABLE_EVENT+" WHERE id=? ;";

    private Connection dbConn;
    private final String connectionString;
    private final String username;
    private final String password;

    public DatabaseManager() {
        connectionString = DEFAULT_FILE;
        username = DEFAULT_USER;
        password = "";
    }

    public DatabaseManager(String dbFile, String user, String password) {
        connectionString = dbFile;
        username = user;
        this.password = password;
    }

    /**
     * Establish the database connection
     * @throws SQLException if the credentials are incorrect
     */
    public void open() throws SQLException {
        dbConn = DriverManager.getConnection(connectionString, username, password);
        createTableIfNotExist();
    }

    /**
     * Explicitly close the database connection
     */
    public void close() {
        try{
            dbConn.close();
            logger.debug("Database connection closed");
        } catch(SQLException sqle) {
            logger.warn("Exception while closing connection: "+sqle.getMessage());
        } catch(NullPointerException npe) {
            logger.warn("Connection doesn't exist");
        }
    }

    /**
     * Insert an Event into the Event table.
     * @param event the data holding object
     * @throws SQLException if the insertion fails
     */
    public void insertEvent(Event event) throws SQLException {
        try {
            PreparedStatement prep = dbConn.prepareStatement(INSERT_EVENT_STATEMENT);
            prep.setString(1, event.getId());
            prep.setLong(2, event.getDuration());
            prep.setString(3, event.getHost());
            prep.setString(4, event.getType());
            prep.setInt(5, event.getAlertAsInt());
            prep.executeUpdate();
            prep.close();
        } catch (SQLException sqle) {
            logger.error("Failure to insert event: "+sqle.getMessage());
            throw sqle;
        }
    }

    /**
     * Retrieve an event by the id and create an Event data holding object
     * @param id the event id (String)
     * @return Event (or null if none is found)
     * @throws SQLException if the query fails
     */
    public Event retrieveEventById(String id) throws SQLException {
        try {
            PreparedStatement prep = dbConn.prepareStatement(SELECT_EVENT);
            prep.setString(1, id);
            ResultSet rs = prep.executeQuery();
            if(!rs.next()) return null;

            return new Event(
                    rs.getString("id"),
                    rs.getLong("duration"),
                    rs.getString("host"),
                    rs.getString("type"),
                    rs.getBoolean("alert")
            );

        } catch (SQLException sqle) {
            logger.error("Failure to retrieve event: "+sqle.getMessage());
            throw sqle;
        }
    }

    private void createTableIfNotExist() {
        try {
            Statement statement = dbConn.createStatement();
            statement.executeUpdate(TABLE_EVENT_CREATION);
            statement.executeUpdate(INDEX_CREATION);
            statement.close();
            logger.info("Event table created");
        } catch (SQLException sqle) {
            logger.info("Existing event table detected");
        }
    }
}
