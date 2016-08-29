package taks1;

public class ConsoleRepository implements Repository {
    @Override
    public void save(String catalog, String table, String value) {
        System.out.printf("Каталог - %s; Таблица - %s; Значение - %s\n", catalog, table, value);
    }
}
