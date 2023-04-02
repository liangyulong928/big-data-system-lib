import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * Experiment One: Big data storage system programming.
 *
 * In this experiment, data in HDFS is stored in HBase after HashJoin operation.
 *
 * run code like: java Hw1Grp0 R=/hw1/nation.tbl S=/hw1/customer.tbl join:R2=S3
 * res:R1,S4,S5
 *
 * I set HDFS_BASE_PATH= "hdfs://localhost:9000". If you change the port of
 * Hadoop, please change this parameter.
 *
 * @author Liang Yulong, 202228013329016, liangyulong22@mails.ucas.ac.cn
 * @since 16 March 2023
 */

public class Hw1Grp0 {

    private static final String HDFSUri = "hdfs://localhost:9000";
    private static String file_R_Uri;
    private static String file_S_Uri;
    private static int joinKeyForR;
    private static int joinKeyForS;
    private static List<String> resList = new ArrayList<String>();
    private static final String tableName = "Result";
    private static final String column_family = "res";
    private static Hashtable<String, Integer> joinKeyCountMap = new Hashtable<String, Integer>();

    /**
     * Location parameters are processed and stored.
     *
     * @param args the positional parameters at project run time
     */
    public void paramProcessing(String[] args) {
        file_R_Uri = args[0].split("=")[1];
        file_S_Uri = args[1].split("=")[1];
        joinKeyForR = Integer.parseInt(args[2].split(":")[1].split("=")[0].substring(1));
        joinKeyForS = Integer.parseInt(args[2].split(":")[1].split("=")[1].substring(1));
        resList = Arrays.asList(args[3].split(":")[1].split(","));
    }

    /**
     * The segmented data is stored using HashTable
     *
     * @param lines the The HDFS record of word segmentation is complete
     * @return the hashTable stored on the record list according to the JoinKey key
     *         value
     */
    public Hashtable<String, List<String[]>> HDFSToHashTable(List<String> lines) {
        Hashtable<String, List<String[]>> stringListHashtable = new Hashtable<String, List<String[]>>();
        for (String line : lines) {
            String[] split = line.split("\\|");
            if (stringListHashtable.get(split[joinKeyForR]) != null) {
                List<String[]> strings = stringListHashtable.get(split[joinKeyForR]);
                strings.add(split);
                stringListHashtable.put(split[joinKeyForR], strings);
            } else {
                ArrayList<String[]> strings = new ArrayList<>();
                strings.add(split);
                stringListHashtable.put(split[joinKeyForR], strings);
            }
        }
        return stringListHashtable;
    }

    /**
     * Read files in the HDFS system and divide fields in records into words
     *
     * @param file_Uri the storage path recorded in the HDFS system
     * @return the list of records that complete word segmentation for a file
     * @throws IOException        the io exception
     * @throws URISyntaxException the uri syntax exception
     */
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

    /**
     * Hash join the two records that can be joined
     *
     * @param recordForR     the record that can be joined in table r
     * @param recordForS     the record that can be joined in table s
     * @param projectionForR the list of projected fields in Table r
     * @param projectionForS the list of projected fields in Table s
     * @param table          the target table stored in HBase
     * @throws NullPointerException the null pointer exception
     * @throws IOException          the io exception
     */
    public void hashJoinForOneRecord(String[] recordForR, String[] recordForS, ArrayList<Integer> projectionForR,
            ArrayList<Integer> projectionForS, HTable table) throws NullPointerException, IOException {
        joinKeyCountMap.putIfAbsent(recordForR[joinKeyForR], 0);
        Put put = new Put(recordForR[joinKeyForR].getBytes());
        for (Integer res_R : projectionForR) {
            put.add(column_family.getBytes(),
                    joinKeyCountMap.get(recordForR[joinKeyForR]) != 0
                            ? ("R" + res_R + "." + joinKeyCountMap.get(recordForR[joinKeyForR])).getBytes()
                            : ("R" + res_R).getBytes(),
                    recordForR[res_R].getBytes());
        }
        for (Integer res_S : projectionForS) {
            put.add(column_family.getBytes(),
                    joinKeyCountMap.get(recordForR[joinKeyForR]) != 0
                            ? ("S" + res_S + "." + joinKeyCountMap.get(recordForR[joinKeyForR])).getBytes()
                            : ("S" + res_S).getBytes(),
                    recordForS[res_S].getBytes());
        }
        table.put(put);
        joinKeyCountMap.put(recordForR[joinKeyForR], joinKeyCountMap.get(recordForR[joinKeyForR]) + 1);
    }

    /**
     * HashJoin the two tables
     *
     * @param R             the table r that has been hashed, key is the join key of
     *                      table R, and value is the list of all records
     *                      corresponding to the join key
     * @param strings_S     the list of all records in Table S that have completed
     *                      word segmentation
     * @param configuration the HBase configuration information
     * @throws MasterNotRunningException    the master not running exception
     * @throws ZooKeeperConnectionException the zoo keeper connection exception
     * @throws IOException                  the io exception
     */
    public void hashJoin(Hashtable<String, List<String[]>> R, List<String> strings_S, Configuration configuration)
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HTable table = new HTable(configuration, tableName);
        ArrayList<Integer> projectionForR = new ArrayList<>();
        ArrayList<Integer> projectionForS = new ArrayList<>();
        for (String res : resList) {
            if (res.charAt(0) == 'R') {
                projectionForR.add(Integer.valueOf(res.substring(1)));
            } else if (res.charAt(0) == 'S') {
                projectionForS.add(Integer.valueOf(res.substring(1)));
            } else {
                System.out.println(res + " is illegal projection term, it is automatically skipped.");
            }
        }
        for (String line : strings_S) {
            String[] split = line.split("\\|");
            if (R.get(split[joinKeyForS]) != null) {
                List<String[]> strings = R.get(split[joinKeyForS]);
                for (String[] record : strings) {
                    hashJoinForOneRecord(record, split, projectionForR, projectionForS, table);
                }
            }
        }
        table.close();
        System.out.println("put successfully");
    }

    /**
     * Configure the HBase system
     *
     * @return the HBase configuration information
     * @throws MasterNotRunningException the master not running exception
     * @throws IOException               the io exception
     */
    public Configuration HBaseConfiguration() throws MasterNotRunningException, IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor res = new HColumnDescriptor(column_family);
        hTableDescriptor.addFamily(res);
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            hAdmin.createTable(hTableDescriptor);
            System.out.println("table " + tableName + " update successfully");
        } else {
            hAdmin.createTable(hTableDescriptor);
            System.out.println("table " + tableName + " created successfully");
        }
        hAdmin.close();
        return configuration;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException                  the io exception
     * @throws NullPointerException         the null pointer exception
     * @throws URISyntaxException           the uri syntax exception
     * @throws MasterNotRunningException    the master not running exception
     * @throws ZooKeeperConnectionException the zoo keeper connection exception
     */
    public static void main(String[] args) throws IOException, NullPointerException, URISyntaxException,
            MasterNotRunningException, ZooKeeperConnectionException {
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
        hw1Grp0.hashJoin(stringListHashtable, stringsForS, configuration);
    }
}