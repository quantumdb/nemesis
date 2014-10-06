package io.quantumdb.nemesis.schema;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.quantumdb.nemesis.schema.utils.DeepCopyable;
import io.quantumdb.nemesis.schema.utils.PrettyStringWriter;
import lombok.Data;


@Data
public class Table implements DeepCopyable<Table> {

	private String name;
	private final List<Column> columns = Lists.newArrayList();

	public Table(String name) {
		this.name = name;
	}
	
	@Override
	public Table copy() {
		Table copy = new Table(name);
		columns.stream().forEachOrdered(column -> copy.columns.add(column.copy()));
		return copy;
	}

	public Table addColumn(Column column) {
		columns.add(column);
		return this;
	}

	public Table addColumns(Collection<Column> columns) {
		this.columns.addAll(columns);
		return this;
	}

	public Column getColumn(String columnName) {
		return columns.stream()
				.filter(c -> c.getName().equals(columnName))
				.findFirst()
				.orElse(null);
	}

	public boolean containsColumn(String columnName) {
		return columns.stream()
				.filter(c -> c.getName().equals(columnName))
				.findFirst()
				.isPresent();
	}

	public void removeColumn(String columnName) {
		columns.remove(getColumn(columnName));
	}

	public ImmutableList<Column> getColumns() {
		return ImmutableList.copyOf(columns);
	}

	public void rename(String newName) {
		this.name = newName;
	}
	
	@Override
	public String toString() {
		return new PrettyStringWriter()
				.append("Table [" + name + "] {\n")
				.modifyIndent(1)
				.append(columns.stream()
						.map(column -> column.toString())
						.collect(Collectors.joining(",\n")))
				.modifyIndent(-1)
				.append("\n}")
				.toString();
	}
	
}
