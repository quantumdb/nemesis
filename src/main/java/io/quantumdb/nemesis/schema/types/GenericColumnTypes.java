package io.quantumdb.nemesis.schema.types;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GenericColumnTypes {
	
	public static enum Type {
		TEXT,
		VARCHAR,
		INT1,
		INT2,
		INT4,
		INT8;
	}
	
	public static ColumnType varchar(int length) {
		return new ColumnType(Type.VARCHAR, length, null);
	}
	
	public static ColumnType text() {
		return new ColumnType(Type.TEXT, null, null);
	}
	
	public static ColumnType int2() {
		return new ColumnType(Type.INT2, null, null);
	}
	
	public static ColumnType int4() {
		return new ColumnType(Type.INT4, null, null);
	}
	
	public static ColumnType int8() {
		return new ColumnType(Type.INT8, null, null);
	}
	
}