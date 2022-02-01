package org.processor;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
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

    private final Gson gson = new Gson();
    private final HashMap<String, LogEntry> entryMap = new HashMap<>();
    private final DatabaseManager dbManager;

    public Processor() {
        dbManager = new DatabaseManager();
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
            LineIterator lit = FileUtils.lineIterator(logFile, "UTF-8");
            while(lit.hasNext()){
                String rawLine = lit.nextLine();
                logger.debug("Raw line read: "+rawLine);
                LogEntry entry = processLine(rawLine);
                if(entry == null) continue;

                if(entryMap.containsKey(entry.getId())) {
                    logger.debug("Calculating event time");
                    String currentId = entry.getId();
                    LogEntry previousEntry = entryMap.get(currentId);
                    long eventDuration = Math.abs(entry.getTimestamp() - previousEntry.getTimestamp());
                    boolean alert = eventDuration > ALERT_THRESHOLD;
                    Event event = new Event(currentId, eventDuration, entry.getHost(), entry.getType(), alert);
                    try{
                        logger.debug("Inserting event: "+event);
                        dbManager.insertEvent(event);
                        count++;
                    } catch (SQLException sqle) {
                        logger.error("Failure to insert event");
                    }
                    entryMap.remove(currentId);
                } else {
                    entryMap.put(entry.getId(), entry);
                }

            }
            lit.close();
            logger.info(count+" event(s) logged");
            if(!entryMap.isEmpty()) {
                logger.warn(entryMap.size()+" event(s) have no closure");
            }
        } catch(Exception e) {
            logger.fatal("Error while parsing file: "+e.getMessage());
            throw new ProcessorException("Error while parsing file: ", e);
        } finally {
            dbManager.close();
        }
        logger.info("End of log file processing.");
    }

    /**
     * Parse a log file line and create a LogEntry object. If the format is unreadable or critical information are missing,
     * the method return null and a warning is logged.
     * @param rawLine a single line from the log file (JSON format)
     * @return LogEntry a object holding the information from the parsed line,
     * or null if the line could not be deciphered.
     */
    public LogEntry processLine(String rawLine) {
        try{
            LogEntry entry = gson.fromJson(rawLine, LogEntry.class);
            logger.debug("Read entry: "+entry.toString());
            if(entry.getId().isEmpty() || entry.getId() == null) throw new ProcessorException("Id missing");
            if(entry.getState().isEmpty() || entry.getState() == null) throw new ProcessorException("State missing");
            if(entry.getTimestamp() <= -1) throw new ProcessorException("Timestamp incoherent or missing");
            return entry;
        } catch (Exception e) {
            logger.warn("Skipping malformed line: \""+rawLine+"\", "+e.getMessage());
            return null;
        }
    }

    private void openDatabaseConnection() throws ProcessorException {
        try{
            dbManager.open();
            logger.info("Database connection open: ");
        } catch (SQLException sqle) {
            logger.fatal("Could not initialize HSQLDB: "+sqle.getMessage());
            throw new ProcessorException("Could not initialize HSQLDB: ", sqle);
        }
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
        if(arg.length > 1 && !arg[1].isEmpty()) testPath = arg[1];
        else testPath = "C:\\Projects\\LogProcessor\\src\\test\\resources\\logFile.txt";

        try {
            Processor P = new Processor();
            P.parseFile(testPath);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
