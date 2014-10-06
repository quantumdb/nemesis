package io.quantumdb.nemesis.schema;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.schema.utils.PrettyStringWriter;
import lombok.Data;

@Data
public class Schema {

	private final String name;
	private final List<Table> tables = Lists.newArrayList();
	
	@Override
	public String toString() {
		return new PrettyStringWriter()
				.append("Schema [" + name + "] {")
				.modifyIndent(1)
				.append(tables.stream()
						.map(table -> table.toString())
						.collect(Collectors.joining(",\n")))
				.modifyIndent(-1)
				.append("}")
				.toString();
	}
	
}
