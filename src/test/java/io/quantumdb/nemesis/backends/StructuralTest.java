package io.quantumdb.nemesis.backends;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import io.quantumdb.nemesis.structure.Table;
import io.quantumdb.nemesis.structure.TableDefinition;
import io.quantumdb.nemesis.structure.postgresql.PostgresDatabase;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Slf4j
@RunWith(Parameterized.class)
public class StructuralTest {

	public static final String TABLE_NAME = "test";

	@Parameterized.Parameters(name = "{index} - {0}")
	public static List<Object[]> listParameters() {
		return Arrays.asList(new Object[][] {
				{ new PostgresDatabase(), "jdbc:postgresql://localhost/profiler", "profiler", "profiler" }
		});
	}

	private final Database database;
	private final String url;
	private final String user;
	private final String pass;

	public StructuralTest(Database database, String url, String user, String pass) {
		this.database = database;
		this.url = url;
		this.user = user;
		this.pass = pass;
	}

	@Before
	public void setUp() throws SQLException {
		DatabaseCredentials credentials = new DatabaseCredentials(url, user, pass);
		database.connect(credentials);
		database.dropContents();
	}

	@After
	public void tearDown() throws SQLException {
		database.dropContents();
		database.close();
	}

	@Test
	public void testTableCreation() throws SQLException {
		database.createTable(new TableDefinition(TABLE_NAME)
				.withColumn(new ColumnDefinition("id", "bigint")
						.isIdentity(true)
						.isNullable(false)
						.isAutoIncrement(true))
				.withColumn(new ColumnDefinition("name", "varchar(255)")
						.isNullable(false)));

		Table table = database.getTable("test");
		Assert.assertNotNull(table.getColumn("id"));
		Assert.assertNotNull(table.getColumn("name"));
	}

	@Test
	public void testTableRenaming() throws SQLException {
		testTableCreation();

		database.getTable("test")
				.rename("test2");

		Assert.assertFalse(database.hasTable("test"));
		Assert.assertTrue(database.hasTable("test2"));
	}

	@Test
	public void testTableDropping() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME).drop();

		Assert.assertTrue(database.listTables().isEmpty());
	}

	@Test
	public void testAddingNullableColumn() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME).addColumn(new ColumnDefinition("city", "varchar(255)")
				.setNullable(true));

		Assert.assertTrue(database.getTable(TABLE_NAME)
				.getColumn("city")
				.isNullable());
	}

	@Test
	public void testAddingIdentityColumn() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME).addColumn(new ColumnDefinition("other_id", "bigint")
				.setIdentity(true)
				.setNullable(false)
				.setDefaultExpression("'1'"));

		Assert.assertTrue(database.getTable(TABLE_NAME)
				.getColumn("other_id")
				.isIdentity());
	}

	@Test
	public void testAddingAutoIncrementColumn() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME).addColumn(new ColumnDefinition("sequence_number", "bigint")
				.setAutoIncrement(true)
				.setNullable(false));

		Assert.assertTrue(database.getTable(TABLE_NAME)
				.getColumn("sequence_number")
				.isAutoIncrement());
	}

	@Test
	public void testAddingColumnWithDefaultExpression() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME).addColumn(new ColumnDefinition("blocked", "boolean")
				.setNullable(false)
				.setDefaultExpression("'false'"));

		Assert.assertEquals("false", database.getTable(TABLE_NAME)
				.getColumn("blocked")
				.getDefaultExpression());
	}

	@Test
	public void testDroppingColumn() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME)
				.getColumn("name")
				.drop();

		Assert.assertFalse(database.getTable(TABLE_NAME)
				.hasColumn("name"));
	}

	@Test
	public void testMakingColumnNullable() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME)
				.getColumn("name")
				.setNullable(true);

		Assert.assertTrue(database.getTable(TABLE_NAME)
				.getColumn("name")
				.isNullable());
	}

	@Test
	public void testMakingColumnNonNullable() throws SQLException {
		testMakingColumnNullable();
		database.getTable(TABLE_NAME)
				.getColumn("name")
				.setNullable(false);

		Assert.assertFalse(database.getTable(TABLE_NAME)
				.getColumn("name")
				.isNullable());
	}

	@Test
	public void testSettingDefaultExpressionOnColumn() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME)
				.getColumn("name")
				.setDefaultExpression("\'NONE\'");

		Assert.assertNotNull(database.getTable(TABLE_NAME)
				.getColumn("name")
				.getDefaultExpression());
	}

	@Test
	public void testDroppingDefaultExpressionOnColumn() throws SQLException {
		testSettingDefaultExpressionOnColumn();
		database.getTable(TABLE_NAME)
				.getColumn("name")
				.setDefaultExpression(null);

		Assert.assertNull(database.getTable(TABLE_NAME)
				.getColumn("name")
				.getDefaultExpression());
	}

	@Test
	public void testMakingColumnAnIdentityColumn() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME)
				.getColumn("name")
				.setIdentity(true);

		Assert.assertTrue(database.getTable(TABLE_NAME)
				.getColumn("name")
				.isIdentity());
	}

	@Test
	public void testCreatingConstraint() throws SQLException {
		testTableCreation();

		database.getTable(TABLE_NAME)
				.addColumn(new ColumnDefinition("counter", "bigint")
						.setNullable(false));

		database.getTable(TABLE_NAME)
				.createConstraint("simple_constraint", "CHECK", "(counter > 0)");

		Assert.assertTrue(database.getTable(TABLE_NAME)
				.hasConstraint("simple_constraint"));
	}

	@Test
	public void testDroppingConstraint() throws SQLException {
		testCreatingConstraint();

		database.getTable(TABLE_NAME)
				.getConstraint("simple_constraint")
				.drop();

		Assert.assertFalse(database.getTable(TABLE_NAME)
				.hasConstraint("simple_constraint"));
	}

	@Test
	public void testCreatingNonUniqueIndex() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME)
				.createIndex("test_name_idx", false, "name");

		Assert.assertNotNull(database.getTable(TABLE_NAME)
				.getIndex("test_name_idx"));
	}

	@Test
	public void testCreatingUniqueIndex() throws SQLException {
		testTableCreation();
		database.getTable(TABLE_NAME)
				.createIndex("test_name_idx", true, "name");

		Assert.assertNotNull(database.getTable(TABLE_NAME)
				.getIndex("test_name_idx"));
	}

	@Test
	public void testDroppingIndex() throws SQLException {
		testCreatingNonUniqueIndex();
		database.getTable(TABLE_NAME)
				.getIndex("test_name_idx")
				.drop();

		Assert.assertFalse(database.getTable(TABLE_NAME)
				.hasIndex("test_name_idx"));
	}

	@Test
	public void testRenamingIndex() throws SQLException {
		testCreatingNonUniqueIndex();
		database.getTable(TABLE_NAME)
				.getIndex("test_name_idx")
				.rename("test_name2_idx");

		Assert.assertFalse(database.getTable(TABLE_NAME)
				.hasIndex("test_name_idx"));
		Assert.assertTrue(database.getTable(TABLE_NAME)
				.hasIndex("test_name2_idx"));
	}

}
