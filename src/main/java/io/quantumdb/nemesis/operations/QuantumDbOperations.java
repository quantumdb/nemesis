package io.quantumdb.nemesis.operations;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.text;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.addForeignKey;
import static io.quantumdb.core.schema.operations.SchemaOperations.alterColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.createIndex;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropIndex;
import static io.quantumdb.core.schema.operations.SchemaOperations.renameTable;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import io.quantumdb.nemesis.structure.Index;
import io.quantumdb.nemesis.structure.TableDefinition;
import io.quantumdb.nemesis.structure.Trigger;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class QuantumDbOperations {

	private abstract static class QuantumDbOperation implements Operation {

		abstract void createChangeSet(Changelog changelog);

		@Override
		public void prepare(Database backendDatabase) throws Exception {
			Backend backend = createBackend(backendDatabase);
			prepareQuantumDb(backend);
		}

		@Override
		public void perform(Database backendDatabase) throws Exception {
			Backend backend = createBackend(backendDatabase);
			State state = prepareQuantumDb(backend);
			Changelog changelog = state.getChangelog();

			createChangeSet(changelog);
			backend.persistState(state);

			backend.getMigrator().migrate(state, changelog.getRoot(), changelog.getLastAdded());
		}

		@Override
		public void cleanup(Database backendDatabase) throws Exception {
			while (true) {
				List<String> tableNames = backendDatabase.listTables().stream()
						.map(io.quantumdb.nemesis.structure.Table::getName)
						.filter(name -> !name.equals("users"))
						.collect(Collectors.toList());

				while (!tableNames.isEmpty()) {
					try {
						backendDatabase.query("DROP TABLE " + tableNames.remove(0) + " CASCADE");
					}
					catch (SQLException e) {
						log.warn(e.getMessage(), e);
					}
				}

				if (tableNames.isEmpty()) {
					break;
				}
			}

			List<Trigger> triggers = backendDatabase.getTable("users").listTriggers();
			for (Trigger trigger : triggers) {
				trigger.drop();
			}
		}

		private State prepareQuantumDb(Backend backend) throws SQLException {
			State state = backend.loadState();
			Catalog catalog = state.getCatalog();
			Changelog changelog = state.getChangelog();

			// Register pre-existing tables in root version.
			TableMapping mapping = state.getTableMapping();
			Version root = changelog.getRoot();
			for (Table table : catalog.getTables()) {
				mapping.add(root, table.getName(), table.getName());
			}

			return state;
		}

		private Backend createBackend(Database backendDatabase) {
			DatabaseCredentials credentials = backendDatabase.getCredentials();
			String url = credentials.getUrl();
			String database = credentials.getDatabase();
			String user = credentials.getUsername();
			String pass = credentials.getPassword();

			Config config = new Config();
			config.setUrl(url);
			config.setUser(user);
			config.setPassword(pass);
			config.setCatalog(database);
			config.setDriver("org.postgresql.Driver");
			return config.getBackend();
		}
	}

	public List<NamedOperation> all() {
		return Lists.newArrayList(
				createIndexOnColumn(),
				dropIndexOnColumn(),
				addNullableColumn(),
				addNonNullableColumn(),
				dropNullableColumn(),
				dropNonNullableColumn(),
				renameNullableColumn(),
				renameNonNullableColumn(),
				modifyDataTypeOnNullableColumn(),
				modifyDataTypeOnNonNullableColumn(),
				modifyDataTypeFromIntToText(),
				setDefaultExpressionOnNullableColumn(),
				setDefaultExpressionOnNonNullableColumn(),
				makeColumnNullable(),
				makeColumnNonNullable(),
				addNonNullableForeignKey(),
				addNullableForeignKey(),
				renameTableInDatabase()
		);
	}

	private NamedOperation createIndexOnColumn() {
		return new NamedOperation("create-index-on-column", new QuantumDbOperation() {
			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Create index", createIndex("users", false, "name"));
			}
			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				for (Index index : backendDatabase.getTable("users").listIndices()) {
					if (!index.isPrimary()) {
						index.drop();
					}
				}
				super.cleanup(backendDatabase);
			}
		});
	}

	private NamedOperation dropIndexOnColumn() {
		return new NamedOperation("drop-index-on-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws SQLException {
				backendDatabase.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Drop index", dropIndex("users", "name"));
			}
		});
	}

	private NamedOperation renameTableInDatabase() {
		return new NamedOperation("rename-table", new QuantumDbOperation() {
			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Renaming table", renameTable("users", "users_v2"));
			}
		});
	}

	public NamedOperation addNullableColumn() {
		return new NamedOperation("add-nullable-column", new QuantumDbOperation() {
			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Adding nullable column", addColumn("users", "email",
						varchar(255)));
			}
		});
	}

	public NamedOperation addNonNullableColumn() {
		return new NamedOperation("add-non-nullable-column", new QuantumDbOperation() {
			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Adding non-nullable column",
						addColumn("users", "life_story", varchar(255), "'Simple story'", NOT_NULL));
			}
		});
	}

	public NamedOperation dropNullableColumn() {
		return new NamedOperation("drop-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Dropping nullable column", dropColumn("users", "email"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation dropNonNullableColumn() {
		return new NamedOperation("drop-non-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Dropping nullable column", dropColumn("users", "email"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation renameNullableColumn() {
		return new NamedOperation("rename-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Renaming nullable column", alterColumn("users", "email").rename(
						"email2"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation renameNonNullableColumn() {
		return new NamedOperation("rename-non-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Renaming non-nullable column", alterColumn("users", "email").rename("email2"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation modifyDataTypeOnNullableColumn() {
		return new NamedOperation("modify-data-type-on-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Modifying data type of nullable column",
						alterColumn("users", "email").modifyDataType(text()));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation modifyDataTypeFromIntToText() {
		return new NamedOperation("modify-data-type-from-int-to-text", new QuantumDbOperation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("cnt", "bigint")
						.setDefaultExpression("8")
						.setNullable(false));
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Modifying data type from int to text",
						alterColumn("users", "cnt").modifyDataType(text()));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("cnt").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation modifyDataTypeOnNonNullableColumn() {
		return new NamedOperation("modify-data-type-on-non-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Modify data type on non-nullable column",
						alterColumn("users", "email").modifyDataType(text()));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.DEFAULT_VALUE_FOR_TEXT);
			}
		});
	}

	public NamedOperation setDefaultExpressionOnNullableColumn() {
		return new NamedOperation("set-default-expression-on-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Set default expression on nullable column",
						alterColumn("users", "email").modifyDefaultExpression("\'SOMETHING ELSE\'"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation setDefaultExpressionOnNonNullableColumn() {
		return new NamedOperation("set-default-expression-on-non-nullable-column", new QuantumDbOperation() {
			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Set default expression on non-nullable column",
						alterColumn("users", "email").modifyDefaultExpression("\'SOMETHING ELSE\'"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation makeColumnNullable() {
		return new NamedOperation("make-column-nullable", new QuantumDbOperation() {

			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Make column nullable",
						alterColumn("users", "email").dropHint(NOT_NULL));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation makeColumnNonNullable() {
		return new NamedOperation("make-column-non-nullable", new QuantumDbOperation() {

			@Override
			public void prepare(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Make column non-nullable",
						alterColumn("users", "email")
								.modifyDefaultExpression("\'NOT_SET\'")
								.addHint(NOT_NULL));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("email").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation addNonNullableForeignKey() {
		return new NamedOperation("add-non-nullable-foreign-key", new QuantumDbOperation() {

			@Override
			public void prepare(Database backendDatabase) throws Exception {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backendDatabase.createTable(table);
				backendDatabase.query("INSERT INTO addresses (address) VALUES ('Unknown');");
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint")
						.setDefaultExpression("'1'")
						.setNullable(false));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Add non-nullable foreign key",
						addForeignKey("users", "address_id").referencing("addresses", "id"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("address_id").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

	public NamedOperation addNullableForeignKey() {
		return new NamedOperation("add-nullable-foreign-key", new QuantumDbOperation() {

			@Override
			public void prepare(Database backendDatabase) throws Exception {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backendDatabase.createTable(table);
				backendDatabase.query("INSERT INTO addresses (address) VALUES ('Unknown');");
				backendDatabase.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint"));

				super.prepare(backendDatabase);
			}

			@Override
			void createChangeSet(Changelog changelog) {
				changelog.addChangeSet("Michael de Jong", "Add nullable foreign key",
						addForeignKey("users", "address_id").referencing("addresses", "id"));
			}

			@Override
			public void cleanup(Database backendDatabase) throws Exception {
				backendDatabase.getTable("users").getColumn("address_id").drop();
				super.cleanup(backendDatabase);
			}
		});
	}

}
