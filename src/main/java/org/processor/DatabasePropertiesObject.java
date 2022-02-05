package org.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Jerome Gerakis
 * Load up informations from database.properties
 */
public class DatabasePropertiesObject {

    public static final String PROPERTIES_FILE = "build/resources/main/database.properties";
    private static final String DEFAULT_FILE = "jdbc:hsqldb:file:serverlogdb";
    private static final String DEFAULT_USER = "SA";
    private static final String DEFAULT_PSWD = "";

    public final String connectionString;
    public final String username;
    public final String passwd;

    public final static Logger logger = LogManager.getLogger(DatabasePropertiesObject.class);

    public DatabasePropertiesObject() {
        String connectionString, username, passwd;
        try{
            String path = System.getProperty("user.dir") + File.separator + PROPERTIES_FILE.replace('/',File.separatorChar);
            InputStream input = new FileInputStream(path);
            Properties prop = new Properties();
            prop.load(input);

            connectionString = prop.getProperty("db.connectionString");
            username = prop.getProperty("db.username");
            passwd = prop.getProperty("db.password");
            input.close();
        } catch (IOException ioe) {
            logger.warn("Failed to read properties file, reverting to default values");
            connectionString = DEFAULT_FILE;
            username = DEFAULT_USER;
            passwd = DEFAULT_PSWD;
        }
        this.connectionString = connectionString;
        this.username = username;
        this.passwd = passwd;
    }
}
