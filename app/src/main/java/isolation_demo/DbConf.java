package isolation_demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DbConf {
    public final String userName;
    public final String password;
    public final String url;

    public DbConf() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File("db.properties")));
        } catch (IOException e) {
            Logger.error(e.toString());
        }

        userName = props.getProperty("user");
        password = props.getProperty("password");
        url = props.getProperty("url");
    }
}