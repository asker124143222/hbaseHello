import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
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

        helper = HBaseHelper.getHBaseHelper(conf);
    }


    //user表插入测试数据
    private void insertUserData() throws IOException{
        // 取得数据表对象

        Table table = helper.getConnection().getTable(TableName.valueOf("user"));

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
        Table table = helper.getConnection().getTable(TableName.valueOf(tableNameString));

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

                System.out.println(s1+",  "+s2+",  "+s3+"\t"+cell.getTimestamp());

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

        helper.close();
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

        helper.bulkInsert(tableNameString,mapList);

        System.out.println(".........批量插入数据end.........");
    }

    private void insertByRowKey(String table,String rowKey) throws IOException{
        Put put = new Put(Bytes.toBytes(rowKey));

        String columnFamily ;
        String columnName ;
        String columnValue ;
        for(int i=0;i<10;i++){
            columnFamily = "info";
            columnName = "username"+i;
            columnValue = "user111";
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            columnFamily = "ex";
            columnName = "addr"+i;
            columnValue = "street 111";
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            columnFamily = "memo";
            columnName = "detail"+i;
            columnValue = "sssss zzz 111222 ";
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
        }
        System.out.println("----> put size:"+put.size());

        helper.put(table,put);

    }

    private void bulkInsertTestTable2(String tableNameString) throws IOException{
//        String tableNameString = "testtable";
        if(!helper.existsTable(tableNameString)){
            helper.createTable(tableNameString,"info","ex","memo");
        }

        List<Put> puts = new ArrayList<>();
        for(int i=0;i<10;i++){
            String rowKey = "rowKey"+i;
            Put put = new Put(Bytes.toBytes(rowKey));

            String columnFamily = "info";
            String columnName = "username2";
            String columnValue = "user"+i;
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            columnFamily = "ex";
            columnName = "addr2";
            columnValue = "street "+i;
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            columnFamily = "memo";
            columnName = "detail2";
            columnValue = "aazzdd "+i;
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));

            System.out.println("put size:"+put.size());
            puts.add(put);
        }
        helper.bulkInsert2(tableNameString,puts);
    }

    private void dumpTable(String tableNameString) throws IOException{
        helper.dump(tableNameString);
        helper.close();
    }

    private void deleteByKey() throws IOException{
        String tableNameString = "testtable";
        String rowKey = "rowKey0";
        helper.deleteByKey(tableNameString,rowKey);
    }

    private void deleteByKeyAndFamily() throws IOException{
        String tableNameString = "testtable";
        String rowKey = "rowKey1";
        String columnFamily="ex";
        helper.deleteByKeyAndFamily(tableNameString,rowKey,columnFamily);
    }

    private void deleteByKeyAndFC() throws IOException{
        String tableNameString = "testtable";
        String rowKey = "rowKey3";
        String columnFamily="ex";
        List<String> list = new ArrayList<>();
//        list.add("addr");
        list.add("addr2");
        helper.deleteByKeyAndFC(tableNameString,rowKey,columnFamily,list);
    }

    private void createTableBySplitKey() throws IOException{
        String tableNameString = "testtable2";
        byte[][] splitKeys = {
                Bytes.toBytes("10"),
                Bytes.toBytes("60"),
                Bytes.toBytes("120"),
        };
        helper.createTable(tableNameString,splitKeys,"info","ex","memo");
    }

    private void getDataByRowKey(String table,String rowKey) throws IOException{
        List<Cell> cells = helper.getRowByKey(table,rowKey);
        dumpCells(rowKey,cells);
    }

    private void getDataByRowKeyFilter(String table,String rowKey) throws IOException{
        Map<String,List<Cell>> map = helper.filterByRowKeyRegex(table,rowKey, CompareOperator.EQUAL);
        for(Map.Entry<String,List<Cell>> entry: map.entrySet()){
            String key = entry.getKey();
            List<Cell> list = entry.getValue();
            dumpCells(key,list);
        }
    }

    private void getDataByValueFilter(String table,String family,String colName,String colValue) throws IOException{
        Map<String,List<Cell>> map = helper.filterByValueRegex(table,family,colName,colValue,CompareOperator.EQUAL);
        for(Map.Entry<String,List<Cell>> entry: map.entrySet()){
            String key = entry.getKey();
            List<Cell> list = entry.getValue();
            dumpCells(key,list);
        }
    }

    private void getDataByColumnPrefix(String table,String prefix) throws IOException{
        Map<String,List<Cell>> map = helper.filterByColumnPrefix(table,prefix);
        for(Map.Entry<String,List<Cell>> entry:map.entrySet()){
            String key = entry.getKey();
            List<Cell> list = entry.getValue();
            dumpCells(key,list);
        }
    }

    private void dumpCells(String key,List<Cell> list){
        for(Cell cell:list){
            String columnFamily = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
            String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            System.out.printf("[key:%s]\t[family:%s] [column:%s] [value:%s]\n",
                    key,columnFamily,columnName,value);
        }
    }


    private void getDataByComplexCol(String table,String colPrefix,String minCol,String maxCol) throws IOException{
        Map<String,List<Cell>> map = helper.filterByPrefixAndRange(table,colPrefix,minCol,maxCol);
        for(Map.Entry<String,List<Cell>> entry:map.entrySet()){
            String key = entry.getKey();
            List<Cell> list = entry.getValue();
            dumpCells(key,list);
        }
    }

    public static void main(String[] args) throws IOException{
//        System.out.println("字符编码："+System.getProperty("file.encoding"));
        MyHBase myHBase = new MyHBase();
        myHBase.setUp();
//        myHBase.createTableBySplitKey();
//        myHBase.deleteByKey();
//        myHBase.deleteByKeyAndFamily();
//        myHBase.deleteByKeyAndFC();
//        myHBase.bulkInsertTestTable2("testtable2");
//        myHBase.queryAll("user");
//        myHBase.queryAll("testtable");
//        myHBase.dumpTable("testtable2");
//        myHBase.insertByRowKey("testtable2","rowKey0");
//        myHBase.getDataByRowKey("testtable2","rowKey0");
//        myHBase.getDataByRowKeyFilter("testtable2","Key1$");
//        myHBase.getDataByRowKeyFilter("user","^power");
//        myHBase.getDataByValueFilter("user","info","username","战士1");

//        myHBase.getDataByColumnPrefix("user","ad");
        myHBase.getDataByComplexCol("testtable2","username0","username0","username9");
        myHBase.close();


    }
}
