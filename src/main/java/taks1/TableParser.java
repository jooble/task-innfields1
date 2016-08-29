package taks1;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableParser implements Runnable {
    private ConnectionFactory factory;
    private DatabaseMetaData metaData;
    private String catalog;
    private String table;
    private Repository repository;

    public TableParser(ConnectionFactory factory, Repository repository, DatabaseMetaData metaData, String catalog, String table) {
        this.factory = factory;
        this.repository = repository;
        this.metaData = metaData;
        this.catalog = catalog;
        this.table = table;
    }

    @Override
    public void run() {
        try (Connection connection = factory.getConnection()) {
            connection.setCatalog(catalog);

            List<String> columns = findColumns();

            String columnNames = columns.stream().collect(Collectors.joining(", "));

            PreparedStatement statement = connection.prepareStatement(String.format("SELECT %s FROM %s", columnNames, table));
            try (ResultSet resultSet = statement.executeQuery()) {

                while (resultSet.next()) {

                    for (String column : columns) {
                        String data = resultSet.getString(column);
                        if (checkLine(data)) {
                            repository.save(catalog, table, data);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<String> findColumns() throws SQLException {
        List<String> columns = new ArrayList<>();
        try (ResultSet resultColumns = metaData.getColumns(catalog, null, table, null)) {
            while (resultColumns.next()) {
                String name = resultColumns.getString("COLUMN_NAME");
                String type = resultColumns.getString("TYPE_NAME");

                if (checkColumnType(type)) {
                    columns.add(name);
                }
            }
            return columns;
        }
    }

    private boolean checkColumnType(String type) {
        return type.equals("TEXT");
    }

    private boolean checkLine(String line) {
        line = line.trim();
        int lineLength = line.length();

        if (lineLength == 10 || lineLength == 12) {
            Pattern pattern = Pattern.compile("\\d+");
            Matcher matcher = pattern.matcher(line);

            int start = 0;
            int quantity = 0;

            while (matcher.find(start)) {
                quantity += line.substring(matcher.start(), matcher.end()).length();
                start = matcher.end();
            }

            int percent = (quantity * 100) / line.length();

            if (percent >= 90) {
                return true;
            }
        }
        return false;
    }

    public DatabaseMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(DatabaseMetaData metaData) {
        this.metaData = metaData;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
