import com.github.dbunit.rules.DBUnitRule;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import org.dbunit.database.DatabaseConfig;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.jooq.example.flyway.db.h2.Tables.AUTHOR;
import static org.jooq.example.flyway.db.h2.Tables.BOOK;
import static org.junit.Assert.assertEquals;

/**
 * Created by Lukas on 23.06.2014.
 */
public class JooqDBUnitTest {

    private static String DB_URL = "jdbc:h2:" + Paths.get("target").toAbsolutePath().toString() +
            "/flyway-test";

    private static Connection connection = createConnection();

    private static Flyway flyway;

    private static Connection createConnection() {
        try {
            return DriverManager.getConnection(DB_URL, "sa", "");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.
            instance(() -> flyway.getDataSource().getConnection());


    @BeforeClass
    public static void initMigration() {
        flyway = new Flyway();
        flyway.setDataSource(DB_URL, "sa", "");
        flyway.setLocations("filesystem:src/main/resources/db/migration");
        flyway.migrate();

        //adds support for schema in datasets
        System.setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, Boolean.TRUE.toString());
    }

    @AfterClass
    public static void cleanMigration() throws SQLException {
        flyway.clean();
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @DataSet("authors.yml,books.yml")
    public void shouldListAuthorsAndBooks() {
        Result<?> result =
                DSL.using(connection)
                        .select(
                                AUTHOR.FIRST_NAME,
                                AUTHOR.LAST_NAME,
                                BOOK.ID,
                                BOOK.TITLE
                        )
                        .from(AUTHOR)
                        .join(BOOK)
                        .on(AUTHOR.ID.eq(BOOK.AUTHOR_ID))
                        .orderBy(BOOK.ID.asc())
                        .fetch();

        assertEquals(4, result.size());
    }


    @Test
    @DataSet(cleanBefore = true, tableOrdering = {"flyway_test.book", "flyway_test.author"})
    public void shouldClearDataBaseUsingSequenceOrder() throws Exception {
        int size = countAuthors() + countBooks();
        assertEquals(0, size);
    }


    @Test
    @DataSet(cleanBefore = true, disableConstraints = true)
    public void shouldClearDataBaseDisablingConstraints() throws Exception {
        DSLContext dsl = DSL.using(connection);
        int size = dsl.fetchCount(AUTHOR);
        assertEquals(0, size);
    }

    @Test
    @DataSet("empty.yml")
    public void shouldClearDataBaseUsingEmptyDataSet() throws Exception {
        DSLContext dsl = DSL.using(connection);
        int size = dsl.fetchCount(AUTHOR);
        assertEquals(0, size);
    }


    public static int countAuthors() {
        return DSL.using(connection).fetchCount(AUTHOR);
    }

    public static int countBooks() {
        return DSL.using(connection).fetchCount(BOOK);
    }
}
