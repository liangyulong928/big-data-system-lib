import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Hw1Grp0 {

    private static final String HDFSUri = "hdfs://localhost:9000";
    private static String file_R_Uri;
    private static String file_S_Uri;
    private static int joinKeyForR;
    private static int joinKeyForS;
    private static List<String> resList = new ArrayList<String>();

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

    public static void main(String[] args) throws IOException, URISyntaxException{
        if (args.length != 4) {
            System.out.println("Usage:R=<file 1> S=<file 2> join:R2=S3 res:R4,S5");
            System.exit(1);
        }
        Hw1Grp0 hw1Grp0 = new Hw1Grp0();
        hw1Grp0.paramProcessing(args);
        List<String> strings = hw1Grp0.readHDFSTable(file_R_Uri);
        Hashtable<String, List<String[]>> stringListHashtable = hw1Grp0.HDFSToHashTable(strings);
        System.out.println(stringListHashtable);
    }
}
