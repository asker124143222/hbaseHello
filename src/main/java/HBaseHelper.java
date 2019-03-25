import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: xu.dm
 * @Date: 2019/3/24 18:23
 * @Description:
 */
public class HBaseHelper implements Closeable {

    private Configuration configuration = null;
    private Connection connection = null;
    private Admin admin = null;

    protected HBaseHelper(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.connection = ConnectionFactory.createConnection(this.configuration);
        admin = this.connection.getAdmin();
    }

    public static HBaseHelper getHBaseHelper(Configuration configuration) throws IOException {
        return new HBaseHelper(configuration);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    public Connection getConnection() {
        return connection;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void createNamespace(String namespace) {
        try {
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void dropNamespace(String namespace, boolean force) {
        try {
            if (force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public boolean existsTable(String table)
            throws IOException {
        return existsTable(TableName.valueOf(table));
    }

    public boolean existsTable(TableName table)
            throws IOException {
        return admin.tableExists(table);
    }

    public void createTable(String table, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, null, colfams);
    }

    public void createTable(TableName table, String... colfams)
            throws IOException {
        createTable(table, 1, null, colfams);
    }

    public void createTable(String table, int maxVersions, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), maxVersions, null, colfams);
    }

    public void createTable(TableName table, int maxVersions, String... colfams)
            throws IOException {
        createTable(table, maxVersions, null, colfams);
    }

    public void createTable(String table, byte[][] splitKeys, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, splitKeys, colfams);
    }

    public void createTable(TableName table, int maxVersions, byte[][] splitKeys,
                            String... colfams)
            throws IOException {
        //表描述器构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);

        //列族描述起构造器
        ColumnFamilyDescriptorBuilder cfDescBuilder;

        //列描述器
        ColumnFamilyDescriptor cfDesc;


        for (String cf : colfams) {
            cfDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            cfDescBuilder.setMaxVersions(maxVersions);
            cfDesc = cfDescBuilder.build();
            tableDescriptorBuilder.setColumnFamily(cfDesc);
        }
        //获得表描述器
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        if (splitKeys != null) {
            admin.createTable(tableDescriptor, splitKeys);
        } else {
            admin.createTable(tableDescriptor);
        }
    }

    public void disableTable(String table) throws IOException {
        disableTable(TableName.valueOf(table));
    }

    public void disableTable(TableName table) throws IOException {
        admin.disableTable(table);
    }

    public void dropTable(String table) throws IOException {
        dropTable(TableName.valueOf(table));
    }

    public void dropTable(TableName table) throws IOException {
        if (existsTable(table)) {
            if (admin.isTableEnabled(table)) disableTable(table);
            admin.deleteTable(table);
        }
    }

    public void put(String table, String row, String fam, String qual,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, val);
    }

    public void put(TableName table, String row, String fam, String qual,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    public void put(String table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, ts, val);
    }

    public void put(TableName table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts,
                Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    public void put(String table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        put(TableName.valueOf(table), rows, fams, quals, ts, vals);
    }

    public void put(TableName table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        Table tbl = connection.getTable(table);
        for (String row : rows) {
            Put put = new Put(Bytes.toBytes(row));
            for (String fam : fams) {
                int v = 0;
                for (String qual : quals) {
                    String val = vals[v < vals.length ? v : vals.length - 1];
                    long t = ts[v < ts.length ? v : ts.length - 1];
                    System.out.println("Adding: " + row + " " + fam + " " + qual +
                            " " + t + " " + val);
                    put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
                            Bytes.toBytes(val));
                    v++;
                }
            }
            tbl.put(put);
        }
        tbl.close();
    }


    public void dump(String table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        dump(TableName.valueOf(table), rows, fams, quals);
    }

    public void dump(TableName table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        Table tbl = connection.getTable(table);
        List<Get> gets = new ArrayList<Get>();
        for (String row : rows) {
            Get get = new Get(Bytes.toBytes(row));
//            get.setMaxVersions();
            if (fams != null) {
                for (String fam : fams) {
                    for (String qual : quals) {
                        get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
                    }
                }
            }
            gets.add(get);
        }
        Result[] results = tbl.get(gets);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell +
                        ", Value: " + Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()));
            }
        }
        tbl.close();
    }

    public void dump(String table) throws IOException {
        dump(TableName.valueOf(table));
    }

    public void dump(TableName table) throws IOException {
        try (
                Table t = connection.getTable(table);
                ResultScanner scanner = t.getScanner(new Scan())
        ) {
            for (Result result : scanner) {
                dumpResult(result);
            }
        }
    }

    public void dumpResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell +
                    ", Value: " + Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }
    }


    //批量插入数据,list里每个map就是一条数据
    public void bulkInsert(TableName tableName, List<Map<String, Object>> list) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        Table table = connection.getTable(tableName);
        if (list != null && list.size() > 0) {
            for (Map<String, Object> map : list) {
                Put put = new Put(Bytes.toBytes(map.get("rowKey").toString()));
                put.addColumn(Bytes.toBytes(map.get("columnFamily").toString()),
                        Bytes.toBytes(map.get("columnName").toString()),
                        Bytes.toBytes(map.get("columnValue").toString()));
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
    }

    public void bulkInsert2(TableName tableName, List<Put> puts) throws IOException {
        Table table = connection.getTable(tableName);
        if (puts != null && puts.size() > 0) {
            table.put(puts);
        }
        table.close();
    }

    public void insert(TableName tableName, Put put) throws IOException {
        Table table = connection.getTable(tableName);
        if (put != null && put.size() > 0) {
            table.put(put);
        }
        table.close();
    }


}
