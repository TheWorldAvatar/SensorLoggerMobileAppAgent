package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/** One servlet-owned pool, shared by ownership checks and all recording tasks. */
final class UploadConnectionPool {
    private UploadConnectionPool() { }

    static HikariDataSource create(String url, String user, String password, int workers) {
        return new HikariDataSource(configure(url, user, password, workers));
    }

    static HikariConfig configure(String url, String user, String password, int workers) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("sensorlogger-uploads");
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(workers);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(10000);
        config.setValidationTimeout(5000);
        config.setIdleTimeout(300000);
        config.setMaxLifetime(1800000);
        // Starting while PostgreSQL is down must not prevent durable upload acceptance.
        config.setInitializationFailTimeout(-1);
        config.addDataSourceProperty("connectTimeout", "10");
        config.addDataSourceProperty("socketTimeout", "60");
        config.addDataSourceProperty("tcpKeepAlive", "true");
        return config;
    }
}
