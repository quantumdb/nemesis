package io.quantumdb.nemesis.structure.postgresql;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Sequence;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class PostgresSequence implements Sequence {

	private final PostgresDatabase parent;
	private final String name;

	PostgresSequence(PostgresDatabase parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public PostgresDatabase getParent() {
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
