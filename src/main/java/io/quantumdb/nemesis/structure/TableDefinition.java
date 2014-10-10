package io.quantumdb.nemesis.structure;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Data;

@Data
public class TableDefinition {

	private final String name;
	private final List<ColumnDefinition> columns;

	public TableDefinition(String name) {
		this.name = name;
		this.columns = Lists.newArrayList();
	}

	public TableDefinition withColumn(ColumnDefinition column) {
		this.columns.add(column);
		return this;
	}

	public ImmutableList<ColumnDefinition> getColumns() {
		return ImmutableList.copyOf(columns);
	}

}
