import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: xu.dm
 * @Date: 2019/3/23 21:36
 * @Description:
 */
public class MyHBase {
    // 与HBase数据库的连接对象
    Connection connection;

    // 数据库元数据操作对象
    Admin admin;

    Configuration conf;

    HBaseHelper helper;


    //初始化
    private void setUp() throws IOException{
        conf = HBaseConfiguration.create();
        conf.set("hbase.master","192.168.31.10");
        //The port the HBase Master should bind to.
//        conf.set("hbase.master.port","16000");

        //The port for the HBase Master web UI. Set to -1 if you do not want a UI instance run.
//        conf.set("hbase.master.info.port","16010");

        //The port the HBase RegionServer binds to.
//        conf.set("hbase.regionserver.port","16020");

        //The port for the HBase RegionServer web UI Set to -1 if you do not want the RegionServer UI to run.
//        conf.set("hbase.regionserver.info.port","16030");

        conf.set("hbase.zookeeper.quorum","192.168.31.10");

        //Property from ZooKeeper’s config zoo.cfg. The port at which the clients will connect.
        // HBase数据库使用的端口
        //conf.set("hbase.zookeeper.property.clientPort", "2181");

        //单机
        conf.set("hbase.rootdir","file:///opt/hbase_data");
        conf.set("hbase.zookeeper.property.dataDir","/opt/hbase_data/zookeeper");

        connection = ConnectionFactory.createConnection(conf);

        admin = connection.getAdmin();

        helper = HBaseHelper.getHBaseHelper(conf);
    }


    //user表插入测试数据
    private void insertUserData() throws IOException{
        // 取得数据表对象
        Table table = connection.getTable(TableName.valueOf("user"));

        // 需要插入数据库的数据集合

        List<Put> putList = new ArrayList<Put>();

        Put put;

        // 生成数据集合
        for(int i = 0; i < 10; i++){
            put = new Put(Bytes.toBytes("user" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes("中文战士" + i));
            put.addColumn(Bytes.toBytes("ex"), Bytes.toBytes("addr"), Bytes.toBytes("北京路1088号：" + i));

            putList.add(put);
        }

        // 将数据集合插入到数据库
        table.put(putList);
        table.close();


    }

    //查询导出所有数据
    private void queryAll(String tableNameString) throws IOException{
        System.out.println("导出数据："+tableNameString);
        // 取得数据表对象
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        // 取得表中所有数据
        ResultScanner scanner = table.getScanner(new Scan());

        for(Result result:scanner){
            byte[] row = result.getRow();
            System.out.println("row key is:" + new String(row));

            List<Cell> cellList = result.listCells();
            for(Cell cell:cellList){
                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();

                String s1 = "row familyArray is:" + Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String s2 = "row qualifierArray is:" + Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String s3 = "row value is:" + Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());

                System.out.println(s1+", "+s2+", "+s3);

                System.out.println();

                //直接使用出现乱码，要使用Bytes类，并且需要使用offset和length
//                System.out.println("row value is:" + new String(familyArray, StandardCharsets.UTF_8) + new String(qualifierArray,StandardCharsets.UTF_8)
//                        + new String(valueArray,StandardCharsets.UTF_8));
            }
        }
        scanner.close();
        table.close();

        System.out.println("导出结束...");
        System.out.println();
    }

    private void close() throws IOException{

        connection.close();
    }

    //插入testtable表数据
    private void initTestTable() throws IOException{
        String tableNameString = "testtable";
        if(helper.existsTable(tableNameString)){
            helper.disableTable(tableNameString);
            helper.dropTable(tableNameString);
        }

        helper.createTable(tableNameString,"info","ex","memo");
        helper.put(tableNameString,"row1","info","username","admin");
        helper.put(tableNameString,"row1","ex","addr","北京大道");
        helper.put(tableNameString,"row1","memo","detail","超级用户，地址：北京大道");


        helper.put(tableNameString,"row2","info","username","guest");
        helper.put(tableNameString,"row2","ex","addr","全国各地");
        helper.put(tableNameString,"row2","memo","detail","游客，地址：全国到处都是");

        helper.close();
    }

    private void bulkInsertTestTable() throws IOException{
        String tableNameString = "testtable";
        if(!helper.existsTable(tableNameString)){
            helper.createTable(tableNameString,"info","ex","memo");
        }

        System.out.println(".........批量插入数据start.........");
        List<Map<String,Object>> mapList = new ArrayList<>();
        for(int i=1;i<201;i++){
            Map<String,Object> map = new HashMap<>();
            map.put("rowKey","testKey"+i);
            map.put("columnFamily","info");
            map.put("columnName","username");
            map.put("columnValue","guest"+i);

            map.put("rowKey","testKey"+i);
            map.put("columnFamily","ex");
            map.put("columnName","addr");
            map.put("columnValue","北京路"+i+"号");

            map.put("rowKey","testKey"+i);
            map.put("columnFamily","memo");
            map.put("columnName","detail");
            map.put("columnValue","联合国地球村北京路第"+i+"号");

            mapList.add(map);
        }

        helper.bulkInsert(TableName.valueOf(tableNameString),mapList);

        System.out.println(".........批量插入数据end.........");
    }

    private void bulkInsertTestTable2() throws IOException{
        String tableNameString = "testtable";
        if(!helper.existsTable(tableNameString)){
            helper.createTable(tableNameString,"info","ex","memo");
        }

        List<Put> puts = new ArrayList<>();
        for(int i=0;i<10;i++){
            String rowKey = "rowKey"+i;
            String columnFamily = "info";
            String columnName = "username";
            String columnValue = "user"+i;
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            rowKey = "rowKey"+i;
            columnFamily = "ex";
            columnName = "addr";
            columnValue = "street "+i;
//            Put put2 = new Put(Bytes.toBytes(rowKey));
//            put2.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            rowKey = "rowKey"+i;
            columnFamily = "memo";
            columnName = "detail";
            columnValue = "aazzdd "+i;
//            Put put3 = new Put(Bytes.toBytes(rowKey));
//            put3.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            System.out.println("put size:"+put.size());
            puts.add(put);
//            puts.add(put2);
//            puts.add(put3);
        }
        helper.bulkInsert2(TableName.valueOf(tableNameString),puts);
    }

    private void dumpTable(String tableNameString) throws IOException{
        helper.dump(tableNameString);
        helper.close();
    }

    public static void main(String[] args) throws IOException{
//        System.out.println("字符编码："+System.getProperty("file.encoding"));
        MyHBase myHBase = new MyHBase();
        myHBase.setUp();
        myHBase.bulkInsertTestTable2();
//        myHBase.queryAll("user");
        myHBase.queryAll("testtable");
//        myHBase.dumpTable("testtable");
        myHBase.close();


    }
}
