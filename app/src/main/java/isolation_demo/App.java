package isolation_demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class App {
    static DbConf db = new DbConf();
    static String name1;
    static String name2;

    static void connectAndProcessStatement(Consumer<Statement> f) {
        try (
            Connection connection = DriverManager.getConnection(db.url, db.userName, db.password)) {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30); // set timeout to 30 sec.
            f.accept(statement);
        } catch (SQLException e) {
            Logger.error(e.toString());
        }
    }

    static void reinit() {
        connectAndProcessStatement(statement -> {
            try {
                statement.executeUpdate("drop table if exists person");
                statement.executeUpdate("create table person (id INT, name VARCHAR(100))");
                statement.executeUpdate("insert into person values(1, 'leo')");
                statement.executeUpdate("insert into person values(2, 'yui')");
            } catch (SQLException e) {
                Logger.error(e.toString());
            }
        });
    }

    static void read() {
        connectAndProcessStatement(statement -> {
            try {
                ResultSet rs = statement.executeQuery("select * from person");
                while (rs.next()) {
                    Logger.print("name = " + rs.getString("name"));
                    Logger.print("id = " + rs.getInt("id"));
                }
            } catch (SQLException e) {
                Logger.error(e.toString());
            }
        });
    }

    static void dirtyRead() {
        Thread thread1 = new Thread(() -> {
            connectAndProcessStatement(statement -> {
                ResultSet rs;
                try {
                    statement.executeUpdate("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
                    rs = statement.executeQuery("SELECT name FROM person WHERE id = 1;");
                    rs.next();
                    name1 = rs.getString("name");
                    Thread.sleep(1000);
                    rs = statement.executeQuery("SELECT name FROM person WHERE id = 1;");
                    rs.next();
                    name2 = rs.getString("name");

                } catch (SQLException | InterruptedException e) {
                    Logger.error(e.toString());
                }
            });
        });

        Thread thread2 = new Thread(() -> {
            connectAndProcessStatement(statement -> {
                ResultSet rs;
                try {
                    /*
                    There is no read uncommitted option in postgres.
                    So dirty read can be acchieved via separate transactions.
                    */
                    Thread.sleep(500);
                    rs = statement.executeQuery("SELECT name FROM person WHERE id = 1;");
                    rs.next();
                    String name = rs.getString("name");
                    statement.executeUpdate("UPDATE person SET name = 'aba' WHERE id = 1;");
                    Thread.sleep(500);
                    statement.executeUpdate(
                        String.format("UPDATE person SET name = '%s' WHERE id = 1;", name));
                } catch (SQLException | InterruptedException e) {
                    Logger.error(e.toString());
                }
            });
        });

        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            Logger.error(e.toString());
        }

        Logger.print(name1);
        Logger.print(name2);
    }

    static void nonRepeatableRead() {
        Thread thread1 = new Thread(() -> {
            connectAndProcessStatement(statement -> {
                ResultSet rs;
                try {
                    // Setting REPEATABLE READ fixed non repeatable read
                    statement.executeUpdate("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;");
                    rs = statement.executeQuery("SELECT name FROM person WHERE id = 1;");
                    rs.next();
                    name1 = rs.getString("name");
                    Thread.sleep(1000);
                    rs = statement.executeQuery("SELECT name FROM person WHERE id = 1;");
                    rs.next();
                    name2 = rs.getString("name");
                } catch (SQLException | InterruptedException e) {
                    Logger.error(e.toString());
                }
            });
        });

        Thread thread2 = new Thread(() -> {
            connectAndProcessStatement(statement -> {
                try {
                    Thread.sleep(500);
                    statement.executeUpdate("UPDATE person SET name = 'aba' WHERE id = 1;");
                } catch (SQLException | InterruptedException e) {
                    Logger.error(e.toString());
                }
            });
        });

        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            Logger.error(e.toString());
        }

        Logger.print(name1);
        Logger.print(name2);
    }

    public static void main(String[] args) throws SQLException {
        reinit();
        nonRepeatableRead();
        reinit();
        dirtyRead();
    }
}
