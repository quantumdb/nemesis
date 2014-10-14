package io.quantumdb.nemesis.operations;

import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.ForeignKey;
import io.quantumdb.nemesis.structure.TableDefinition;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Operations {

	public static List<NamedOperation> all() {
		return Lists.newArrayList(
				createIndexOnColumn(),
				dropIndexOnColumn(),
				renameIndexOnColumn(),
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
				addNullableForeignKey()
		);
	}

	public static NamedOperation addNullableColumn() {
		return new NamedOperation("add-nullable-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public static NamedOperation addNonNullableColumn() {
		return new NamedOperation("add-non-nullable-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("life_story", "varchar(255)")
						.setDefaultExpression("'Simple story'")
						.setNullable(false));
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("life_story").drop();
			}
		});
	}

	public static NamedOperation dropNullableColumn() {
		return new NamedOperation("drop-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public static NamedOperation dropNonNullableColumn() {
		return new NamedOperation("drop-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public static NamedOperation renameNullableColumn() {
		return new NamedOperation("rename-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").rename("email2");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email2").drop();
			}
		});
	}

	public static NamedOperation renameNonNullableColumn() {
		return new NamedOperation("rename-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").rename("email2");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email2").drop();
			}
		});
	}

	public static NamedOperation createIndexOnColumn() {
		return new NamedOperation("create-index-on-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").drop();
			}
		});
	}

	public static NamedOperation dropIndexOnColumn() {
		return new NamedOperation("drop-index-on-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").drop();
			}
		});
	}

	public static NamedOperation renameIndexOnColumn() {
		return new NamedOperation("rename-index", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").rename("users_name2_idx");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name2_idx").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.RENAME_INDEX);
			}
		});
	}

	public static NamedOperation modifyDataTypeOnNullableColumn() {
		return new NamedOperation("modify-data-type-on-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setType("text");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public static NamedOperation modifyDataTypeOnNonNullableColumn() {
		return new NamedOperation("modify-data-type-on-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setType("text");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.DEFAULT_VALUE_FOR_TEXT);
			}
		});
	}

	public static NamedOperation modifyDataTypeFromIntToText() {
		return new NamedOperation("modify-data-type-from-int-to-text", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("cnt", "bigint")
						.setDefaultExpression("8")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("cnt").setType("text");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("cnt").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.DEFAULT_VALUE_FOR_TEXT);
			}
		});
	}

	public static NamedOperation setDefaultExpressionOnNullableColumn() {
		return new NamedOperation("set-default-expression-on-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email")
						.setDefaultExpression("\'SOMETHING ELSE\'");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public static NamedOperation setDefaultExpressionOnNonNullableColumn() {
		return new NamedOperation("set-default-expression-on-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setDefaultExpression("\'SOMETHING ELSE\'");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public static NamedOperation makeColumnNullable() {
		return new NamedOperation("make-column-nullable", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setNullable(true);
			}
		});
	}

	public static NamedOperation makeColumnNonNullable() {
		return new NamedOperation("make-column-non-nullable", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setNullable(true);
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setNullable(false);
			}
		});
	}

	public static NamedOperation addNonNullableForeignKey() {
		return new NamedOperation("add-non-nullable-foreign-key", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown');");
				backend.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint")
						.setDefaultExpression("'1'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addForeignKey("users_address", new String[] { "address_id" },
						"addresses", new String[] { "id" });
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getForeignKey("users_address").drop();
				backend.getTable("users").getColumn("address_id").drop();
				backend.getTable("addresses").drop();
			}
		});
	}

	public static NamedOperation addNullableForeignKey() {
		return new NamedOperation("add-nullable-foreign-key", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown');");
				backend.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addForeignKey("users_address", new String[] { "address_id" },
						"addresses", new String[] { "id" });
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getForeignKey("users_address").drop();
				backend.getTable("users").getColumn("address_id").drop();
				backend.getTable("addresses").drop();
			}
		});
	}

}
