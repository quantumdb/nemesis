package io.quantumdb.nemesis.schema;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.schema.utils.PrettyStringWriter;
import lombok.Data;


@Data
public class Catalog {

	private final String name;
	private final List<Schema> schemas = Lists.newArrayList();
	
	@Override
	public String toString() {
		return new PrettyStringWriter()
				.append("Catalog [" + name + "] {")
				.modifyIndent(1)
				.append(schemas.stream()
						.map(schema -> schema.toString())
						.collect(Collectors.joining(",\n")))
				.modifyIndent(-1)
				.append("}")
				.toString();
	}
	
}
