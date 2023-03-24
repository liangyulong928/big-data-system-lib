import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import org.apache.log4j.*;

public class Hw1Grp0 {

    private static final String HDFSUri = "hdfs://localhost:9000";
    private static String file_R_Uri;
    private static String file_S_Uri;
    private static int joinKeyForR;
    private static int joinKeyForS;
    private static List<String> resList = new ArrayList<String>();
    private static final String tableName= "Result";
    private static final String column_family = "res";
    private static Hashtable<String, Integer> joinKeyCountMap = new Hashtable<String, Integer>();

    public void paramProcessing(String[] args){
        file_R_Uri = args[0].split("=")[1];
        file_S_Uri = args[1].split("=")[1];
        joinKeyForR = Integer.parseInt(args[2].split(":")[1].split("=")[0].substring(1));
        joinKeyForS = Integer.parseInt(args[2].split(":")[1].split("=")[1].substring(1));
        resList = Arrays.asList(args[3].split(":")[1].split(","));
    }

    public Hashtable<String,List<String[]>> HDFSToHashTable(List<String> lines){
        Hashtable<String, List<String[]>> stringListHashtable = new Hashtable<String, List<String[]>>();
        for (String line : lines){
            String[] split = line.split("\\|");
            if (stringListHashtable.get(split[joinKeyForR])!=null){
                List<String[]> strings = stringListHashtable.get(split[joinKeyForR]);
                strings.add(split);
                stringListHashtable.put(split[joinKeyForR],strings);
            }
            else {
                ArrayList<String[]> strings = new ArrayList<>();
                strings.add(split);
                stringListHashtable.put(split[joinKeyForR],strings);
            }
        }
        return stringListHashtable;
    }

    public List<String> readHDFSTable(String file_Uri) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFSUri + file_Uri), conf);
        Path path = new Path(HDFSUri + file_Uri);
        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String line = null;
        List<String> strings = new ArrayList<>();
        while ((line = in.readLine()) != null) {
            strings.add(line);
        }
        in.close();
        fs.close();
        return strings;
    }

    public void hashJoinForOneRecord(String[] recordForR,String[] recordForS,ArrayList<Integer> projectionForR,ArrayList<Integer> projectionForS,HTable table) throws NullPointerException,IOException {
        joinKeyCountMap.putIfAbsent(recordForR[joinKeyForR], 0);
        StringBuilder output = new StringBuilder("-join key=" + recordForR[joinKeyForR]);
        Put put = new Put(recordForR[joinKeyForR].getBytes());
        for (Integer res_R:projectionForR){
            output.append(",R").append(res_R).append("=").append(recordForR[res_R]);
            put.add(column_family.getBytes(),
                    joinKeyCountMap.get(recordForR[joinKeyForR])!=0?("R"+res_R+"."+joinKeyCountMap.get(recordForR[joinKeyForR])).getBytes():("R"+res_R).getBytes(),
                    recordForR[res_R].getBytes());
        }
        for (Integer res_S:projectionForS){
            output.append(",S").append(res_S).append("=").append(recordForS[res_S]);
            put.add(column_family.getBytes(),
                    joinKeyCountMap.get(recordForR[joinKeyForR])!=0?("S"+res_S+"."+joinKeyCountMap.get(recordForR[joinKeyForR])).getBytes():("S"+res_S).getBytes(),
                    recordForS[res_S].getBytes());
        }
        table.put(put);
        joinKeyCountMap.put(recordForR[joinKeyForR],joinKeyCountMap.get(recordForR[joinKeyForR]) + 1);
        System.out.println(output);
    }

    public void hashJoin(Hashtable<String, List<String[]>> R,List<String> strings_S,Configuration configuration) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
        HTable table = new HTable(configuration,tableName);
        ArrayList<Integer> projectionForR = new ArrayList<>();
        ArrayList<Integer> projectionForS = new ArrayList<>();
        for (String res : resList){
            if (res.charAt(0) == 'R'){
                projectionForR.add(Integer.valueOf(res.substring(1)));
            }
            if (res.charAt(0) == 'S'){
                projectionForS.add(Integer.valueOf(res.substring(1)));
            }
        }
        for (String line : strings_S){
            String[] split = line.split("\\|");
            if (R.get(split[joinKeyForS])!=null){
                List<String[]> strings = R.get(split[joinKeyForS]);
                for (String[] record : strings){
                    hashJoinForOneRecord(record,split,projectionForR,projectionForS,table);
                }
            }
        }
        table.close();
        System.out.println("put successfully");
    }

    public Configuration HBaseConfiguration() throws MasterNotRunningException, IOException{
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor res = new HColumnDescriptor(column_family);
        hTableDescriptor.addFamily(res);
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            hAdmin.createTable(hTableDescriptor);
            System.out.println("table "+tableName+ " update successfully");
        }
        else {
            hAdmin.createTable(hTableDescriptor);
            System.out.println("table "+tableName+ " created successfully");
        }
        hAdmin.close();
        return configuration;
    }

    public static void main(String[] args) throws IOException, NullPointerException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException{
        Logger.getRootLogger().setLevel(Level.ERROR);
        if (args.length != 4) {
            System.out.println("Usage:R=<file 1> S=<file 2> join:R2=S3 res:R4,S5");
            System.exit(1);
        }
        Hw1Grp0 hw1Grp0 = new Hw1Grp0();
        hw1Grp0.paramProcessing(args);
        List<String> stringsForR = hw1Grp0.readHDFSTable(file_R_Uri);
        Hashtable<String, List<String[]>> stringListHashtable = hw1Grp0.HDFSToHashTable(stringsForR);
        List<String> stringsForS = hw1Grp0.readHDFSTable(file_S_Uri);
        Configuration configuration = hw1Grp0.HBaseConfiguration();
        hw1Grp0.hashJoin(stringListHashtable,stringsForS,configuration);
    }
}
