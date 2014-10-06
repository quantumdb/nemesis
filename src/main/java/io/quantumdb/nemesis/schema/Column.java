package io.quantumdb.nemesis.schema;

import java.util.Arrays;

import com.google.common.base.Strings;
import io.quantumdb.nemesis.schema.types.ColumnType;
import io.quantumdb.nemesis.schema.utils.DeepCopyable;
import lombok.Data;

@Data
public class Column implements DeepCopyable<Column> {
	
	public static enum Hint {
		UNSIGNED, NOT_NULL, AUTO_INCREMENT, PRIMARY_KEY;
	}
	
	private String name;
	private final ColumnType type;
	private final String defaultValueExpression;
	private final Hint[] hints;
	
	public Column(String name, ColumnType type, Hint... hints) {
		this(name, type, null, hints);
	}
	
	public Column(String name, ColumnType type, String defaultValueExpression, Hint... hints) {
		this.name = name;
		this.type = type;
		this.defaultValueExpression = defaultValueExpression;
		this.hints = hints;
	}
	
	public boolean isPrimaryKey() {
		return containsHint(Hint.PRIMARY_KEY);
	}
	
	public boolean isAutoIncrement() {
		return containsHint(Hint.AUTO_INCREMENT);
	}
	
	public boolean isNotNull() {
		return containsHint(Hint.NOT_NULL);
	}
	
	public boolean isUnsigned() {
		return containsHint(Hint.UNSIGNED);
	}
	
	private boolean containsHint(Hint needle) {
		for (Hint hint : hints) {
			if (hint == needle) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Column copy() {
		return new Column(name, type, defaultValueExpression, Arrays.copyOf(hints, hints.length));
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder()
				.append("Column ")
				.append("[" + name + "] " + type);
		
		if (!Strings.isNullOrEmpty(defaultValueExpression)) {
			builder.append(" default: '" + defaultValueExpression + "'");
		}
		
		for (Hint hint : hints) {
			builder.append(" " + hint.name());
		}
		
		return builder.toString();
	}
	
}