package experiment3.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public final class HBaseLab {
    private static final String NAMESPACE = "experiment3";
    private static final String STUDENT = "experiment3:Student";
    private static final String COURSE = "experiment3:Course";
    private static final String SC = "experiment3:SC";
    private static final String DEMO = "experiment3:StudentDemo";
    private static final String[] STUDENT_FIELDS = {
        "Info:S_Name", "Info:S_No", "Info:S_Sex", "Info:S_Age"
    };
    private static final String[] COURSE_FIELDS = {
        "Info:C_No", "Info:C_Name", "Info:C_Credit"
    };
    private static final String[] SC_FIELDS = {
        "Info:SC_Sno", "Info:SC_Cno", "Score:Value"
    };

    private final Connection connection;
    private final BufferedReader input;

    public HBaseLab(Connection connection, BufferedReader input) {
        this.connection = connection;
        this.input = input;
    }

    public void createTable(String tableName, String[] fields) throws IOException {
        TableName name = TableName.valueOf(tableName);
        TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(name);
        Set<String> families = new TreeSet<>();
        for (String field : fields) {
            families.add(new Column(field).family);
        }
        if (families.isEmpty()) {
            throw new IllegalArgumentException("至少需要一个列族");
        }
        for (String family : families) {
            descriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
        }

        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(name)) {
                if (admin.isTableEnabled(name)) {
                    admin.disableTable(name);
                }
                admin.deleteTable(name);
            }
            admin.createTable(descriptor.build());
        }
    }

    public void addRecord(String tableName, String row, String[] fields, String[] values)
            throws IOException {
        if (fields.length == 0 || fields.length != values.length) {
            throw new IllegalArgumentException("字段和值必须一一对应，且至少有一个字段");
        }
        Put put = new Put(Bytes.toBytes(row));
        for (int index = 0; index < fields.length; index++) {
            Column column = new Column(fields[index]);
            put.addColumn(Bytes.toBytes(column.family), Bytes.toBytes(column.qualifier),
                    Bytes.toBytes(values[index]));
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(put);
        }
    }

    public Map<String, Map<String, String>> scanColumn(String tableName, String column)
            throws IOException {
        Column selection = new Column(column);
        byte[] family = Bytes.toBytes(selection.family);
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        Set<String> columnNames = new TreeSet<>();

        // 扫描整行，否则只读取指定列会跳过缺少该列的行。
        try (Table table = connection.getTable(TableName.valueOf(tableName));
                ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                Map<String, String> values = new TreeMap<>();
                if (selection.qualified) {
                    byte[] value = result.getValue(family, Bytes.toBytes(selection.qualifier));
                    values.put(column, value == null ? null : Bytes.toString(value));
                } else {
                    NavigableMap<byte[], byte[]> cells = result.getFamilyMap(family);
                    if (cells != null) {
                        cells.forEach((qualifier, value) -> values.put(
                                selection.family + ":" + Bytes.toString(qualifier),
                                Bytes.toString(value)));
                    }
                    columnNames.addAll(values.keySet());
                }
                rows.put(Bytes.toString(result.getRow()), values);
            }
        }

        if (!selection.qualified) {
            if (columnNames.isEmpty()) {
                columnNames.add(column);
            }
            for (Map<String, String> values : rows.values()) {
                columnNames.forEach(name -> values.putIfAbsent(name, null));
            }
        }
        return rows;
    }

    public void modifyData(String tableName, String row, String column) throws IOException {
        System.out.printf("为 %s/%s 输入新值：%n", row, column);
        System.out.flush();
        String value = input.readLine();
        if (value == null) {
            throw new IOException("标准输入未提供新值");
        }
        addRecord(tableName, row, new String[] {column}, new String[] {value});
    }

    public void deleteRow(String tableName, String row) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(Bytes.toBytes(row)));
        }
    }

    public void listTables() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            for (TableName table : admin.listTableNames()) {
                System.out.println(table.getNameAsString());
            }
        }
    }

    public void scanTable(String tableName) throws IOException {
        System.out.println("扫描 " + tableName);
        try (Table table = connection.getTable(TableName.valueOf(tableName));
                ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result row : scanner) {
                Map<String, String> values = new TreeMap<>();
                for (Cell cell : row.rawCells()) {
                    String column = Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneQualifier(cell));
                    values.put(column, Bytes.toString(CellUtil.cloneValue(cell)));
                }
                System.out.println(Bytes.toString(row.getRow()) + "\t" + values);
            }
        }
    }

    public long countRows(String tableName) throws IOException {
        long count = 0;
        try (Table table = connection.getTable(TableName.valueOf(tableName));
                ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result row : scanner) {
                count++;
            }
        }
        return count;
    }

    public void addColumnFamily(String tableName, String family) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.addColumnFamily(TableName.valueOf(tableName),
                    ColumnFamilyDescriptorBuilder.of(family));
        }
    }

    public void deleteColumnFamily(String tableName, String family) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(family));
        }
    }

    public void deleteColumn(String tableName, String row, String column) throws IOException {
        Column selection = new Column(column);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(Bytes.toBytes(row)).addColumns(
                    Bytes.toBytes(selection.family), Bytes.toBytes(selection.qualifier)));
        }
    }

    public void truncateTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        try (Admin admin = connection.getAdmin()) {
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            }
            admin.truncateTable(name, false);
        }
    }

    private void ensureNamespace() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            try {
                admin.getNamespaceDescriptor(NAMESPACE);
            } catch (NamespaceNotFoundException missing) {
                admin.createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
            }
        }
    }

    private void loadTable(String table, Path file, String[] fields,
            Function<String[], String> rowKey) throws IOException {
        List<String[]> records = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String[] values = line.split("\t", -1);
            if (values.length != fields.length) {
                throw new IOException("TSV 字段数不匹配：" + file + " — " + line);
            }
            records.add(values);
        }
        createTable(table, fields);
        for (String[] values : records) {
            addRecord(table, rowKey.apply(values), fields, values);
        }
    }

    private void load(Path directory) throws IOException {
        loadTable(STUDENT, directory.resolve("student.tsv"), STUDENT_FIELDS, row -> row[0]);
        loadTable(COURSE, directory.resolve("course.tsv"), COURSE_FIELDS, row -> row[0]);
        loadTable(SC, directory.resolve("sc.tsv"), SC_FIELDS, row -> row[0] + "#" + row[1]);
        listTables();
        for (String table : Arrays.asList(STUDENT, COURSE, SC)) {
            scanTable(table);
            System.out.printf("%s 行数：%d%n", table, countRows(table));
        }
    }

    private void demo(Path directory) throws IOException {
        loadTable(DEMO, directory.resolve("student.tsv"), STUDENT_FIELDS, row -> row[0]);
        addColumnFamily(DEMO, "Demo");
        addRecord(DEMO, "Zhangsan", new String[] {"Demo:Note"}, new String[] {"example"});
        System.out.println("指定列：" + scanColumn(DEMO, "Demo:Note"));
        System.out.println("指定列族：" + scanColumn(DEMO, "Demo"));
        deleteColumn(DEMO, "Zhangsan", "Demo:Note");
        System.out.println("删除示例列：" + scanColumn(DEMO, "Demo:Note"));
        deleteColumnFamily(DEMO, "Demo");
        modifyData(DEMO, "Lisi", "Info:S_Age");
        System.out.println("修改年龄：" + scanColumn(DEMO, "Info:S_Age"));
        deleteRow(DEMO, "Mary");
        System.out.println("删除一行后行数：" + countRows(DEMO));
        truncateTable(DEMO);
        System.out.println("清空后行数：" + countRows(DEMO));
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2 || !("load".equals(args[0]) || "demo".equals(args[0]))) {
            throw new IllegalArgumentException("用法：HBaseLab <load|demo> <数据目录>");
        }
        Path directory = Paths.get(args[1]);
        if (!Files.isDirectory(directory)) {
            throw new IOException("数据目录不存在：" + directory);
        }
        Configuration config = HBaseConfiguration.create();
        String confDir = System.getenv("HBASE_CONF_DIR");
        if (confDir != null && !confDir.isEmpty()) {
            Path siteFile = Paths.get(confDir, "hbase-site.xml");
            if (!Files.isRegularFile(siteFile)) {
                throw new IOException("HBase 配置文件不存在：" + siteFile);
            }
            config.addResource(new org.apache.hadoop.fs.Path(siteFile.toUri()));
        }

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            HBaseLab lab = new HBaseLab(connection,
                    new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)));
            lab.ensureNamespace();
            if ("load".equals(args[0])) {
                lab.load(directory);
            } else {
                lab.demo(directory);
            }
        }
    }

    private static final class Column {
        private final String family;
        private final String qualifier;
        private final boolean qualified;

        private Column(String field) {
            String[] parts = field.split(":", 2);
            family = parts[0];
            if (family.isEmpty()) {
                throw new IllegalArgumentException("列族名不能为空：" + field);
            }
            qualified = parts.length == 2;
            qualifier = qualified ? parts[1] : "";
        }
    }
}
