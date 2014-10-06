package io.quantumdb.nemesis.schema.types;

import lombok.Data;

@Data 
public class ColumnType {
	
	private final GenericColumnTypes.Type type;
	private final Integer length;
	private final Integer precision;
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder()
				.append(type);
		
		if (length != null) {
			builder.append("(" + length);
			if (precision != null) {
				builder.append("," + precision);
			}
			builder.append(")");
		}
		
		return builder.toString();
	}
	
}