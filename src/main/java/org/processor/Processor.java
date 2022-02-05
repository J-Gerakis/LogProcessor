package org.processor;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author Jerome Gerakis
 * The class that does the main job: open log file, convert line into objects, save into the DB
 * Can use a specific database, or create a default one if left unspecified
 */
public class Processor {

    public final static Logger logger = LogManager.getLogger(Processor.class);
    public static final long ALERT_THRESHOLD = 4;
    public static final String CHARSET = "UTF-8";

    private final Gson gson = new Gson();
    private final HashMap<String, LogEntry> entryMap = new HashMap<>();
    private final DatabaseManager dbManager;

    public Processor() {
        DatabasePropertiesObject propertiesObject = new DatabasePropertiesObject();
        dbManager = new DatabaseManager(propertiesObject.connectionString, propertiesObject.username, propertiesObject.passwd);
        logger.info("Database manager created");
    }

    public Processor(DatabaseManager databaseManager) {
        this.dbManager = databaseManager;
    }

    /**
     * Open and parse a text file for log entries. Calculate duration between 'started' and 'finished' events and flag
     * if the result is superior to 4ms, then save in the database.
     * @param path path to log file
     * @throws ProcessorException if the file cannot be read, or the database cannot be accessed
     */
    public void parseFile(String path) throws ProcessorException {
        logger.info("Processing file: '"+path+"'");
        File logFile = openFile(path);
        openDatabaseConnection();

        try {
            int count = 0;
            LineIterator lineIterator = FileUtils.lineIterator(logFile, CHARSET);
            while(lineIterator.hasNext()){
                String rawLine = lineIterator.nextLine();
                logger.debug("Raw line read: "+rawLine);
                LogEntry entry;
                try{
                    entry = processLine(rawLine);
                } catch (IllegalArgumentException | JsonSyntaxException e) {
                    logger.warn("Skipping malformed line: \""+rawLine+"\": "+e.getMessage());
                    logger.debug(e);
                    continue;
                }
                if(entryMap.containsKey(entry.getId())) {
                    count += insertIntoDatabase(entry);
                    entryMap.remove(entry.getId());
                } else {
                    entryMap.put(entry.getId(), entry);
                }
            }
            lineIterator.close();
            logger.info(count+" event(s) logged");
            if(!entryMap.isEmpty()) {
                logger.warn(entryMap.size()+" event(s) have no closure");
            }
        } catch(IOException ioe) {
            logger.fatal("Error while parsing file: "+ioe.getMessage());
            throw new ProcessorException("Error while parsing file: ", ioe);
        } finally {
            dbManager.close();
        }
        logger.info("End of log file processing.");
    }

    /**
     * Parse a log file line and create a LogEntry object. At least 3 elements need to be present: id, state, and timestamp
     * @param rawLine a single line from the log file (JSON format)
     * @return LogEntry a object holding the information from the parsed line,
     * @throws IllegalArgumentException if critical information are missing, or JsonSyntaxException
     * if the line cannot be deciphered
     */
    public LogEntry processLine(String rawLine) throws IllegalArgumentException {
        LogEntry entry = gson.fromJson(rawLine, LogEntry.class);
        logger.debug("Read entry: "+entry.toString());
        if(entry.getId() == null || entry.getId().isEmpty()) throw new IllegalArgumentException("Id missing");
        if(entry.getState() == null || entry.getState().isEmpty()) throw new IllegalArgumentException("State missing");
        if(entry.getTimestamp() <= -1) throw new IllegalArgumentException("Timestamp incoherent or missing");
        return entry;
    }

    private void openDatabaseConnection() throws ProcessorException {
        try{
            dbManager.open();
            logger.info("Database connection open");
        } catch (SQLException sqle) {
            logger.fatal("Could not initialize HSQLDB: "+sqle.getMessage());
            throw new ProcessorException("Could not initialize HSQLDB: ", sqle);
        }
    }

    private int insertIntoDatabase(LogEntry entry) {
        logger.debug("Calculating event time");
        String currentId = entry.getId();
        LogEntry previousEntry = entryMap.get(currentId);
        long eventDuration = Math.abs(entry.getTimestamp() - previousEntry.getTimestamp());
        boolean alert = eventDuration > ALERT_THRESHOLD;
        Event event = new Event(currentId, eventDuration, entry.getHost(), entry.getType(), alert);
        try{
            logger.debug("Inserting event: "+event);
            dbManager.insertEvent(event);
            return 1;
        } catch (SQLException sqle) {
            logger.error("Failure to insert event: "+sqle.getMessage());
            logger.debug(sqle);
        }
        return 0;
    }

    private File openFile(String path) throws ProcessorException {
        if(path.isEmpty()) throw new ProcessorException("Provided path is empty");
        File logFile = new File(path);
        if(!logFile.exists()) throw new ProcessorException("Provided path is incorrect, or file doesn't exist: '" +path+"'");
        if(!logFile.canRead()) throw new ProcessorException("File cannot be read, check permissions: '" +path+"'");
        return logFile;
    }

    public static void main(String[] arg) {
        String testPath;
        if(arg.length > 0 && !arg[0].isEmpty()) testPath = arg[0];
        else testPath = "C:\\Projects\\LogProcessor\\src\\test\\resources\\logFile.txt";

        try {
            Processor P = new Processor();
            P.parseFile(testPath);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
