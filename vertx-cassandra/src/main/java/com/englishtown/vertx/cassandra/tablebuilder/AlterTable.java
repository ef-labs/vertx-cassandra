package com.englishtown.vertx.cassandra.tablebuilder;

/**
 * ALTER TABLE CQL3 statement builder
 */
public class AlterTable extends BuiltTableStatement {

    private Column alterColumn;
    private Column addColumn;
    private Column dropColumn;
    private Column renameColumnFrom;
    private Column renameColumnTo;

    AlterTable(String keyspace, String table) {
        super(keyspace, table);
    }

    public AlterTable alterColumn(String column, String type) {
        alterColumn = new Column(column, type, false);
        return this;
    }

    public AlterTable addColumn(String column, String type) {
        addColumn = new Column(column, type, false);
        return this;
    }

    public AlterTable dropColumn(String column) {
        dropColumn = new Column(column, null, false);
        return this;
    }

    public AlterTable renameColumn(String columnFrom, String columnTo) {
        renameColumnFrom = new Column(columnFrom, null, false);
        renameColumnTo = new Column(columnTo, null, false);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuilder buildQueryString() {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER TABLE ");
        if (keyspace != null) {
            sb.append(keyspace).append(".");
        }
        sb.append(table).append(" ");

        if (alterColumn != null) {
            sb.append("ALTER ")
                    .append(alterColumn.getName())
                    .append(" TYPE ")
                    .append(alterColumn.getType());
        } else if (addColumn != null) {
            sb.append("ADD ")
                    .append(addColumn.getName())
                    .append(" ")
                    .append(addColumn.getType());
        } else if (dropColumn != null) {
            sb.append("DROP ")
                    .append(dropColumn.getName());
        } else if (renameColumnFrom != null && renameColumnTo != null) {
            sb.append("RENAME ")
                    .append(renameColumnFrom.getName())
                    .append(" TO ")
                    .append(renameColumnTo.getName());
        }

        return sb;
    }
}
