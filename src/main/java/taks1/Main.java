package taks1;


import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = ConnectionFactory.getInstance();
        DatabaseMetaData metaData = factory.getConnection().getMetaData();

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Repository repository = new ConsoleRepository();
        Main main = new Main();

        List<String> catalogs = main.findCatalogs(metaData);
        Map<String, List<String>> catalogsTables = main.findTables(metaData, catalogs);

        for (Map.Entry<String, List<String>> catalogTable : catalogsTables.entrySet()) {

            String catalog = catalogTable.getKey();
            List<String> tables = catalogTable.getValue();

            for (String table : tables) {
                TableParser parser = new TableParser(factory, repository, metaData, catalog, table);
                executorService.submit(parser);
            }
        }
        executorService.shutdown();
    }

    private List<String> findCatalogs(DatabaseMetaData metaData) throws SQLException {
        List<String> catalogs = new ArrayList<>();
        try (ResultSet resultSet = metaData.getCatalogs()) {
            while (resultSet.next()) {
                String catalog = resultSet.getString(1);
                if (checkCatalog(catalog)) {
                    catalogs.add(catalog);
                }
            }
            return catalogs;
        }
    }

    private Map<String, List<String>> findTables(DatabaseMetaData metaData, List<String> catalogs) throws SQLException {
        Map<String, List<String>> catalogAndTables = new HashMap<>();

        for (String catalog : catalogs) {
            List<String> tables = new ArrayList<>();

            try (ResultSet resultSet = metaData.getTables(catalog, null, null, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String table = resultSet.getString("TABLE_NAME");
                    tables.add(table);
                }
            }
            catalogAndTables.put(catalog, tables);
        }
        return catalogAndTables;
    }

    private boolean checkCatalog(String catalog) {
        if (catalog.equals("mysql")) {
            return false;
        }
        return true;
    }
}