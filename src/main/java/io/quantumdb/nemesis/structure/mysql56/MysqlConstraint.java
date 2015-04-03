package io.quantumdb.nemesis.structure.mysql56;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Constraint;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class MysqlConstraint implements Constraint {

	private final MysqlTable parent;
	private final String name;
	private final String type;
	private final String expression;

	MysqlConstraint(MysqlTable parent, String name, String type, String expression) {
		this.parent = parent;
		this.name = name;
		this.type = type;
		this.expression = expression;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public MysqlTable getParent() {
		return parent;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER ONLINE TABLE %s DROP CONSTRAINT %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

}
