package telegram.files;

import cn.hutool.core.lang.Version;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.*;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.mysqlclient.MySQLBuilder;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnectOptions;
import org.jooq.lambda.tuple.Tuple;
import telegram.files.repository.*;
import telegram.files.repository.impl.FileRepositoryImpl;
import telegram.files.repository.impl.SettingRepositoryImpl;
import telegram.files.repository.impl.StatisticRepositoryImpl;
import telegram.files.repository.impl.TelegramRepositoryImpl;

import java.io.File;
import java.util.List;

public class DataVerticle extends AbstractVerticle {

    private static final Log log = LogFactory.get();

    public static Pool pool;

    public static FileRepository fileRepository;

    public static TelegramRepository telegramRepository;

    public static SettingRepository settingRepository;

    public static StatisticRepository statisticRepository;

    private static SqlConnectOptions sqlConnectOptions;

    public static final List<Definition> definitions;

    static {
        if (Config.isPostgres() || Config.isMysql()) {
            sqlConnectOptions = Config.isPostgres() ? new PgConnectOptions() : new MySQLConnectOptions();
            sqlConnectOptions
                    .setPort(Config.DB_PORT)
                    .setHost(Config.DB_HOST)
                    .setDatabase(Config.DB_NAME)
                    .setUser(Config.DB_USER)
                    .setPassword(Config.DB_PASSWORD);
        }

        definitions = List.of(
                new SettingRecord.SettingRecordDefinition(),
                new TelegramRecord.TelegramRecordDefinition(),
                new FileRecord.FileRecordDefinition(),
                new StatisticRecord.StatisticRecordDefinition()
        );
    }

    public void start(Promise<Void> stopPromise) {
        pool = buildSqlClient();
        settingRepository = new SettingRepositoryImpl(pool);
        telegramRepository = new TelegramRepositoryImpl(pool);
        fileRepository = new FileRepositoryImpl(pool);
        statisticRepository = new StatisticRepositoryImpl(pool);
        isCompletelyNewInitialization()
                .compose(isNew -> Future.all(definitions.stream().map(d -> d.createTable(pool)).toList()).map(isNew))
                .compose(isNew -> settingRepository.<Version>getByKey(SettingKey.version).map(version -> Tuple.tuple(isNew, version)))
                .compose(tuple -> {
                    if (tuple.v1) return Future.succeededFuture();

                    Version version = tuple.v2 == null ? new Version("0.0.0") : tuple.v2;
                    return Future.all(definitions.stream().map(d -> d.migrate(pool, version, new Version(Start.VERSION))).toList());
                })
                .compose(r ->
                        settingRepository.createOrUpdate(SettingKey.version.name(), Start.VERSION))
                // SAFETY: Some legacy DBs (e.g., labeled 0.1.6) may miss columns required by recovery.
                // Ensure critical columns exist before we run startup recovery queries that touch them.
                .compose(r -> ensureFileRecordCoreColumns())
                .compose(r -> {
                    // DESIGN NOTE: Reset stuck downloads on startup to prevent deadlocks
                    log.info("Performing startup recovery - resetting stuck downloads");
                    return fileRepository.resetAllDownloadingToIdle();
                })
                .onSuccess(resetCount -> {
                    log.info("Database {} initialized. Reset {} stuck downloads on startup.", Config.DB_TYPE, resetCount);
                    stopPromise.complete();
                })
                .onFailure(err -> {
                    log.error("Failed to initialize database: %s".formatted(err.getMessage()));
                    stopPromise.fail(err);
                });
    }

    private Future<Void> ensureFileRecordCoreColumns() {
        if (Config.isSqlite()) {
            return pool
                    .query("PRAGMA table_info(file_record);")
                    .execute()
                    .compose(rs -> {
                        java.util.Set<String> cols = new java.util.HashSet<>();
                        rs.forEach(row -> cols.add(row.getString("name")));
                        java.util.List<Future<?>> ops = new java.util.ArrayList<>();
                        if (!cols.contains("download_status")) {
                            ops.add(pool.query("ALTER TABLE file_record ADD COLUMN download_status VARCHAR(255) DEFAULT 'idle';").execute());
                        }
                        if (!cols.contains("downloaded_size")) {
                            ops.add(pool.query("ALTER TABLE file_record ADD COLUMN downloaded_size BIGINT DEFAULT 0;").execute());
                        }
                        if (ops.isEmpty()) {
                            return Future.succeededFuture();
                        }
                        return Future
                                .all(ops)
                                .onFailure(err -> log.error("Failed ensuring file_record columns (sqlite): %s".formatted(err.getMessage())))
                                .mapEmpty();
                    });
        } else if (Config.isPostgres()) {
            return Future
                    .all(
                            pool.query("ALTER TABLE file_record ADD COLUMN IF NOT EXISTS download_status VARCHAR(255) DEFAULT 'idle';").execute(),
                            pool.query("ALTER TABLE file_record ADD COLUMN IF NOT EXISTS downloaded_size BIGINT DEFAULT 0;").execute()
                    )
                    .onFailure(err -> log.error("Failed ensuring file_record columns (postgres): %s".formatted(err.getMessage())))
                    .mapEmpty();
        } else if (Config.isMysql()) {
            return Future
                    .all(
                            pool.query("ALTER TABLE file_record ADD COLUMN IF NOT EXISTS download_status VARCHAR(255) DEFAULT 'idle';").execute(),
                            pool.query("ALTER TABLE file_record ADD COLUMN IF NOT EXISTS downloaded_size BIGINT DEFAULT 0;").execute()
                    )
                    .onFailure(err -> log.error("Failed ensuring file_record columns (mysql): %s".formatted(err.getMessage())))
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        if (pool != null) {
            pool.close().onComplete(r -> {
                if (r.succeeded()) {
                    log.debug("Data verticle stopped!");
                } else {
                    log.error("Failed to close data verticle: %s".formatted(r.cause().getMessage()));
                }
                stopPromise.complete();
            });
        }
    }

    public static String getDataPath() {
        String dataPath = System.getenv("DATA_PATH");
        dataPath = StrUtil.blankToDefault(dataPath, "data.db");
        dataPath = Config.APP_ROOT + File.separator + dataPath;
        return dataPath;
    }

    private Pool buildSqlClient() {
        PoolOptions poolOptions = new PoolOptions()
                .setShared(true)
                .setMaxSize(8)
                .setName("pool-tf")
                .setIdleTimeout(300000)
                .setPoolCleanerPeriod(300000);

        return createPool(vertx,
                Config.isSqlite() ? new JDBCConnectOptions()
                        .setJdbcUrl("jdbc:sqlite:%s?journal_mode=WAL&busy_timeout=30000&synchronous=NORMAL&cache_size=-2000".formatted(getDataPath())) :
                        sqlConnectOptions,
                poolOptions);
    }

    private Future<Boolean> isCompletelyNewInitialization() {
        if (Config.isSqlite()) {
            return pool.query("""
                            SELECT name
                            FROM sqlite_master
                            WHERE type = 'table'
                            ORDER BY name;
                            """)
                    .execute()
                    .map(rs -> rs.size() == 0);
        } else if (Config.isPostgres()) {
            return isCompletelyNewInitializationForPostgres();
        } else if (Config.isMysql()) {
            return isCompletelyNewInitializationForMySQL();
        } else {
            return Future.failedFuture("Unsupported database type");
        }
    }

    private Future<Boolean> isCompletelyNewInitializationForPostgres() {
        SqlClient sqlClient;
        String query;
        if (Config.DB_NEED_CREATE) {
            query = """
                    SELECT 1 FROM pg_database WHERE datname = '%s'
                    """.formatted(Config.DB_NAME);
            sqlClient = createSqlClient(vertx, createDefaultOptions());
        } else {
            query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                    """;
            sqlClient = pool;
        }

        return sqlClient.query(query)
                .execute()
                .map(rs -> rs.size() == 0)
                .compose(isNew -> {
                    if (isNew) {
                        return Config.DB_NEED_CREATE ? sqlClient.query("""
                                        CREATE DATABASE "%s"
                                        """.formatted(Config.DB_NAME))
                                .execute()
                                .map(true) : Future.succeededFuture(true);
                    } else {
                        return Future.succeededFuture(false);
                    }
                })
                .eventually(() -> {
                    if (Config.DB_NEED_CREATE)
                        return sqlClient.close();
                    return Future.succeededFuture();
                })
                .onFailure(err -> log.error("Failed to check database initialization: %s".formatted(err.getMessage())));
    }

    private Future<Boolean> isCompletelyNewInitializationForMySQL() {
        SqlClient sqlClient;
        String query;
        if (Config.DB_NEED_CREATE) {
            query = """
                    SELECT SCHEMA_NAME
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME = '%s'
                    """.formatted(Config.DB_NAME);
            sqlClient = createSqlClient(vertx, createDefaultOptions());
        } else {
            query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '%s'
                    ORDER BY table_name;
                    """.formatted(Config.DB_NAME);
            sqlClient = pool;
        }

        return sqlClient.query(query)
                .execute()
                .map(rs -> rs.size() == 0)
                .compose(isNew -> {
                    if (isNew) {
                        return Config.DB_NEED_CREATE ? sqlClient.query("""
                                        CREATE DATABASE `%s` collate utf8mb4_bin;
                                        """.formatted(Config.DB_NAME))
                                .execute()
                                .map(true) :
                                Future.succeededFuture(true);
                    } else {
                        return Future.succeededFuture(false);
                    }
                })
                .eventually(() -> {
                    if (Config.DB_NEED_CREATE)
                        return sqlClient.close();
                    return Future.succeededFuture();
                })
                .onFailure(err -> log.error("Failed to check database initialization: %s".formatted(err.getMessage())));
    }

    public static SqlConnectOptions getSqlConnectOptions() {
        return sqlConnectOptions;
    }

    public static SqlConnectOptions createDefaultOptions() {
        if (Config.isPostgres()) {
            SqlConnectOptions options = new SqlConnectOptions(sqlConnectOptions.toJson());
            options.setDatabase("postgres");
            return options;
        } else if (Config.isMysql()) {
            SqlConnectOptions options = new SqlConnectOptions(sqlConnectOptions.toJson());
            options.setDatabase("mysql");
            return options;
        } else {
            throw new VertxException("Unsupported database type");
        }
    }

    public static Pool createPool(Vertx vertx, Object connectOptions, PoolOptions poolOptions) {
        if (Config.isSqlite()) {
            return JDBCPool.pool(vertx,
                    (JDBCConnectOptions) connectOptions,
                    poolOptions
            );
        } else if (Config.isPostgres()) {
            return PgBuilder
                    .pool()
                    .using(vertx)
                    .with(poolOptions)
                    .connectingTo((SqlConnectOptions) connectOptions)
                    .build();
        } else if (Config.isMysql()) {
            return MySQLBuilder
                    .pool()
                    .using(vertx)
                    .with(poolOptions)
                    .connectingTo((SqlConnectOptions) connectOptions)
                    .build();
        } else {
            throw new VertxException("Unsupported database type");
        }
    }

    public static SqlClient createSqlClient(Vertx vertx, Object connectOptions) {
        if (Config.isSqlite()) {
            return JDBCPool.pool(vertx, (JDBCConnectOptions) connectOptions, new PoolOptions().setMaxSize(1));
        } else if (Config.isPostgres()) {
            return PgBuilder
                    .client()
                    .using(vertx)
                    .connectingTo((SqlConnectOptions) connectOptions)
                    .build();
        } else if (Config.isMysql()) {
            return MySQLBuilder
                    .client()
                    .using(vertx)
                    .connectingTo((SqlConnectOptions) connectOptions)
                    .build();
        } else {
            throw new VertxException("Unsupported database type");
        }
    }
}
