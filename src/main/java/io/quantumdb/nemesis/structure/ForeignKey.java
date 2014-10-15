package io.quantumdb.nemesis.structure;

import java.sql.SQLException;

public interface ForeignKey {

	String getName();

	void drop() throws SQLException;

}
