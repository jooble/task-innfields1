package taks1;

public class ConsoleRepository implements Repository {
    @Override
    public void save(String catalog, String table, String column, String value) {
        System.out.println(String.format("%s-%s-%s-%s", catalog, table, column, value));
    }
}
