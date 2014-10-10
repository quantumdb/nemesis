package io.quantumdb.nemesis.structure;

import java.sql.SQLException;

public interface Column extends Comparable<Column> {

	String getName();
	void rename(String newName) throws SQLException;

	Table getParent();

	String getType();
	void setType(String type) throws SQLException;

	boolean isNullable();
	void setNullable(boolean nullable) throws SQLException;

	String getDefaultExpression();
	void setDefaultExpression(String expression) throws SQLException;

	boolean isIdentity();
	void setIdentity(boolean isIdentityColumn) throws SQLException;

	boolean isAutoIncrement();

	void drop() throws SQLException;

	default int compareTo(Column other) {
		return getName().compareTo(other.getName());
	};

}
