package io.quantumdb.nemesis.structure.mysql55;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Sequence;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class MysqlSequence implements Sequence {

	private final MysqlDatabase parent;
	private final String name;

	MysqlSequence(MysqlDatabase parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public MysqlDatabase getParent() {
		return parent;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP SEQUENCE %s", name));
	}

	private void execute(String query) throws SQLException {
		getParent().execute(query);
	}

}
