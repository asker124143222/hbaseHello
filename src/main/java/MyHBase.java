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
import java.util.List;

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

        conf.set("hbase.rootdir","file:///opt/hbase_data");
        conf.set("hbase.zookeeper.property.dataDir","/opt/hbase_data/zookeeper");

        connection = ConnectionFactory.createConnection(conf);

        admin = connection.getAdmin();

        helper = HBaseHelper.getHBaseHelper(conf);
    }

    //创建表
    private void createTable() throws IOException{

        String tableNameString = "user";

        TableName tableName = TableName.valueOf(tableNameString);

        if(admin.tableExists(tableName)){
            System.out.println(tableNameString+"已经存在！");
        }else {

        }

    }

    //
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

    private void queryAll(String tableNameString) throws IOException{
        System.out.println("查询："+tableNameString);
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

//                System.out.println("row value is:" + new String(familyArray, StandardCharsets.UTF_8) + new String(qualifierArray,StandardCharsets.UTF_8)
//                        + new String(valueArray,StandardCharsets.UTF_8));
            }
        }
        scanner.close();
        table.close();

        System.out.println("查询结束。。。");
        System.out.println();
    }

    private void close() throws IOException{

        connection.close();
    }

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

    private void dumpTable(String tableNameString) throws IOException{
        helper.dump(tableNameString);
        helper.close();
    }

    public static void main(String[] args) throws IOException{
//        System.out.println("字符编码："+System.getProperty("file.encoding"));
        MyHBase myHBase = new MyHBase();
        myHBase.setUp();
        myHBase.queryAll("user");
        myHBase.queryAll("testtable");
//        myHBase.dumpTable("testtable");
        myHBase.close();


    }
}
