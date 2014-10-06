package io.quantumdb.nemesis.operations;

import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.backends.DatabaseBackend;
import io.quantumdb.nemesis.schema.Column;
import io.quantumdb.nemesis.schema.Column.Hint;
import io.quantumdb.nemesis.schema.Table;
import io.quantumdb.nemesis.schema.types.GenericColumnTypes;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Operations {
	
	public static List<NamedOperation> all() {
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
				addNullableForeignKey()
		);
	}

	public static NamedOperation addNullableColumn() {
		return new NamedOperation("add-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
			}
			
			public void perform(DatabaseBackend backend) throws SQLException {
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255)));
			}
			
			public void cleanup(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
			}
		});
	}
	
	public static NamedOperation addNonNullableColumn() {
		return new NamedOperation("add-non-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "life_story");
			}
			
			public void perform(DatabaseBackend backend) throws SQLException {
				backend.addColumn("users", new Column("life_story", GenericColumnTypes.text(), "'Simple story'", Hint.NOT_NULL));
			}
			
			public void cleanup(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "life_story");
			}
		});
	}
	
	public static NamedOperation dropNullableColumn() {
		return new NamedOperation("drop-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255)));
			}
			
			public void perform(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
			}
			
			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing.
			}
		});
	}
		
	public static NamedOperation dropNonNullableColumn() {
		return new NamedOperation("drop-non-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255), "\'NOT_SET\'", Hint.NOT_NULL));
			}
			
			public void perform(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
			}
			
			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing.
			}
		});
	}

	public static NamedOperation renameNullableColumn() {
		return new NamedOperation("rename-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255)));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.renameColumn("users", "email", "email2");
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email2");
			}
		});
	}

	public static NamedOperation renameNonNullableColumn() {
		return new NamedOperation("rename-non-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255), "\'NOT_SET\'", Hint.NOT_NULL));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.renameColumn("users", "email", "email2");
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email2");
			}
		});
	}

	public static NamedOperation createIndexOnColumn() {
		return new NamedOperation("create-index-on-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
//				backend.dropIndex("users", "name");
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.createIndex("users", "name");
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
//				backend.dropIndex("users", "name");
			}
		});
	}

	public static NamedOperation dropIndexOnColumn() {
		return new NamedOperation("drop-index-on-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
//				backend.dropIndex("users", "name");
//				backend.createIndex("users", "name");
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.dropIndex("users", "name");
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation modifyDataTypeOnNullableColumn() {
		return new NamedOperation("modify-data-type-on-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255)));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.modifyDataType("users", "email", GenericColumnTypes.text());
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation modifyDataTypeOnNonNullableColumn() {
		return new NamedOperation("modify-data-type-on-non-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255), "\'NOT_SET\'", Hint.NOT_NULL));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.modifyDataType("users", "email", GenericColumnTypes.text());
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation modifyDataTypeFromIntToText() {
		return new NamedOperation("modify-data-type-from-int-to-text", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "cnt");
				backend.addColumn("users", new Column("cnt", GenericColumnTypes.int8(), "8", Hint.NOT_NULL));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.modifyDataType("users", "cnt", GenericColumnTypes.text());
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation setDefaultExpressionOnNullableColumn() {
		return new NamedOperation("set-default-expression-on-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255)));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.setDefaultExpression("users", "email", "\'SOMETHING ELSE\'");
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation setDefaultExpressionOnNonNullableColumn() {
		return new NamedOperation("set-default-expression-on-non-nullable-column", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255), "\'NOT_SET\'", Hint.NOT_NULL));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.setDefaultExpression("users", "email", "\'SOMETHING ELSE\'");
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation makeColumnNullable() {
		return new NamedOperation("make-column-nullable", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users", new Column("email", GenericColumnTypes.varchar(255), "\'NOT_SET\'", Hint.NOT_NULL));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.setNullable("users", "email", true);
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation makeColumnNonNullable() {
		return new NamedOperation("make-column-non-nullable", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "email");
				backend.addColumn("users",
						new Column("email", GenericColumnTypes.varchar(255), "\'NOT_SET\'", Hint.NOT_NULL));
				backend.setNullable("users", "email", true);
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.setNullable("users", "email", false);
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				// Do nothing...
			}
		});
	}

	public static NamedOperation addNonNullableForeignKey() {
		return new NamedOperation("add-non-nullable-foreign-key", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				Table table = new Table("addresses")
						.addColumn(new Column("id", GenericColumnTypes.int8(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
						.addColumn(new Column("address", GenericColumnTypes.varchar(255), "\'\'", Hint.NOT_NULL));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown');");
				backend.addColumn("users", new Column("address_id", GenericColumnTypes.int8(), "1", Hint.NOT_NULL));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.addForeignKey("users", new String[] { "address_id" }, "addresses", new String[] { "id" });
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "address_id");
				backend.dropTable("addresses");
			}
		});
	}

	public static NamedOperation addNullableForeignKey() {
		return new NamedOperation("add-nullable-foreign-key", new Operation() {
			public void prepare(DatabaseBackend backend) throws SQLException {
				Table table = new Table("addresses")
						.addColumn(new Column("id", GenericColumnTypes.int8(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
						.addColumn(new Column("address", GenericColumnTypes.varchar(255), "\'\'", Hint.NOT_NULL));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown');");
				backend.addColumn("users", new Column("address_id", GenericColumnTypes.int8()));
			}

			public void perform(DatabaseBackend backend) throws SQLException {
				backend.addForeignKey("users", new String[] { "address_id" }, "addresses", new String[] { "id" });
			}

			public void cleanup(DatabaseBackend backend) throws SQLException {
				backend.dropColumn("users", "address_id");
				backend.dropTable("addresses");
			}
		});
	}

}
