package taks1;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableParser implements Runnable {
    
    private static final String COLUMN_NAME = GlobalCache.intern("COLUMN_NAME");
    private static final String TYPE_NAME = GlobalCache.intern("TYPE_NAME");
    private static final String TEXT = GlobalCache.intern("TEXT");
    private static int counter = 1;
    private final ConnectionFactory factory;
    private DatabaseMetaData metaData;
    private String catalog;
    private String table;
    private final Repository repository;
    private final Pattern pattern = Pattern.compile("\\d+");

    public TableParser(ConnectionFactory factory, Repository repository, DatabaseMetaData metaData, String catalog, String table) {
        this.factory = factory;
        this.repository = repository;
        this.metaData = metaData;
        this.catalog = GlobalCache.intern(catalog);
        this.table = GlobalCache.intern(table);
    }

    @Override
    public void run() {
        try {
            Connection connection = factory.getConnection();
            connection.setCatalog(catalog);

            List<String> columns = findColumns();

            String query = String.format("SELECT * FROM %s", table);
            
            
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                if ((TableParser.counter % 1000) == 0) {
                    System.err.println(GlobalCache.intern(String.format("there is a reading table(%s) base(%s) record #%d", table, catalog, TableParser.counter)));
                }
                    for (String column : columns) {
                        String value = GlobalCache.intern(resultSet.getString(column));
                        if (checkLine(value)) {
                            repository.save(catalog, table, column, value);
                        }
                        TableParser.counter++;
                    }
                }
            resultSet.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<String> findColumns() throws SQLException {
        List<String> columns = new ArrayList<String>();
        ResultSet resultColumns = metaData.getColumns(catalog, null, table, null);
            while (resultColumns.next()) {
                String name = GlobalCache.intern(resultColumns.getString(COLUMN_NAME));
                String type = GlobalCache.intern(resultColumns.getString(TYPE_NAME));

                if (checkColumnType(type)) {
                    columns.add(name);
                }
            }

        resultColumns.close();
        return columns;
        }


    private boolean checkColumnType(String type) {
        return type.equals(TEXT);
    }

    private boolean checkLine(String line) {
        line = GlobalCache.intern(line.trim());
        int lineLength = line.length();

        if (lineLength == 10 || lineLength == 12) {
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
