package edu.usc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.FileInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class VcfLookup {

//    public static final String SER_FILE = "vcf.ser";
    public static String TXT_FILE = "workdir/depth/vcf.txt";

//    private static volatile Map<String, Set<Integer>> hashMapSet = new HashMap<>();
//    private static volatile boolean hashMapSetInitialized = false;
//
    public VcfLookup() {
    }

//    /**
//     * Check to see if a particular SNP exists in the tree structure
//     *
//     * @param key The chromosome
//     * @param value The base pair position
//     * @return true or false
//     */
//    public boolean exists(String key, int value) {
//        Set<Integer> hashSet = hashMapSet.get(key);
//        if (hashSet == null) {
//            return false;
//        }
//        return hashSet.contains(value);
//    }
//
//    /**
//     * Deserializes the tree structure from HDFS into memory
//     */
//    public synchronized void readObject() {
//        System.err.println("Entering readObject");
//        if (hashMapSetInitialized) {
//            System.err.println("Exiting readObject early");
//            return;
//        }
//
//        System.err.println("Loading from serialized file\n");
//        ObjectInputStream ois = null;
//        hashMapSetInitialized = true;
//
//        try {
//            FileSystem fileSystem = FileSystem.get(new Configuration());
//            Path path = new Path(SER_FILE);
//            ois = new ObjectInputStream(fileSystem.open(path));
////            VcfLookup.hashMapSet = (HashMap<String, Set<Integer>>) ois.readObject();
//            hashMapSet = (HashMap<String, Set<Integer>>) ois.readObject();
//        } catch (Exception ex) {
//            System.err.println("Printing the stack trace 1\n");
//            ex.printStackTrace(System.err);
//        } finally {
//            try {
//                if (ois != null) {
//                    ois.close();
//                }
//            } catch (IOException ex) {
//                System.err.println("Printing the stack trace 2\n");
//                ex.printStackTrace(System.err);
//            }
//        }
//        System.err.println("Done reading serialized file");
//        hashMapSetInitialized = true;
//    }
//
//    /**
//     * Insert a data point into the tree structure
//     *
//     * @param key The chromosome
//     * @param value the position
//     */
//    public void insertKeyValue(String key, int value) {
//        Set<Integer> hashSet = hashMapSet.get(key);
//        if (hashSet == null) {
//            hashSet = new HashSet<Integer>();
//        }
//        hashSet.add(value);
//        hashMapSet.put(key, hashSet);
//    }
//
//    /**
//     * Parses a VCF file from the local file system and creates an in-memory
//     * tree structure. It also serializes this tree to HDFS
//     *
//     * @param infile The local file path of the VCF file
//     * @throws java.io.IOException
//     */
//    public void parseVcf(String infile) throws IOException {
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));
//        String line;
//
//        while ((line = reader.readLine()) != null) {
//            if (line.charAt(0) != '#') {
//                String parts[] = line.split("\t");
//                //System.out.println(parts[0]+" "+parts[1]);
//                insertKeyValue(parts[0], Integer.parseInt(parts[1]));
//            }
//        }
//
//        FileSystem fileSystem = FileSystem.get(new Configuration());
//        fileSystem.delete(new Path(SER_FILE), true);
//        Path path = new Path(SER_FILE);
//        ObjectOutputStream oos = new ObjectOutputStream(fileSystem.create(path));
//        oos.writeObject(hashMapSet);
//        System.err.println("VCF file parsed, and serialized object persisted");
//    }
//
    /**
     * Parses a VCF from local file system and stores a tab-delimited file in
     * HDFS at /user/$HOMEDIR/workdir/depth. The Depth call reducers also write
     * to this path. The Mapper for the Binner can then determine which sites
     * are SNPs as the intersection of the VCF file and the output of the Depth
     * call reducers.
     *
     * @param infile The local file path of the VCF
     * @throws java.io.IOException
     */
    public void parseVcf2Text(String infile) throws IOException {
        String bamfileStr = UserConfig.getBamFile();
        File bamFile = new File(bamfileStr);
        String subFolderName = bamFile.getName().substring(0, bamFile.getName().lastIndexOf("."));
        TXT_FILE = TXT_FILE.substring(0, TXT_FILE.lastIndexOf(java.io.File.separator))
                + java.io.File.separator + subFolderName + java.io.File.separator + "vcf.txt";
//        System.out.println("TXT_FILE: " + TXT_FILE);

        String line;
        FileSystem fileSystem = FileSystem.get(new Configuration());
        fileSystem.delete(new Path(TXT_FILE), true);
        Path path = new Path(TXT_FILE);
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));
            writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)));
            while ((line = reader.readLine()) != null) {
                if (line.charAt(0) != '#') {
                    String parts[] = line.split("\t");
                    String ref = parts[0];
                    String pos = parts[1];
                    writer.write(ref + "\t" + pos + "\t0\t0\n");
                }
            }
        } catch (Exception ex) {
            System.err.println("Error in parseVcf2Text, caused by " + ex.toString());
            System.exit(0);
        } finally {
            try {
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception ex) {
                System.err.println("Error in closing writer, caused by " + ex.toString());
            }
        }
        System.err.println("VCF file parsed, and txt object persisted");
    }
//
//    public void testRead() throws IOException, ClassNotFoundException {
////        HashMap<String, Set<Integer>> hashMapSet;
//        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("vcf.ser"));
//        hashMapSet = (HashMap<String, Set<Integer>>) ois.readObject();
//        Iterator<String> it = hashMapSet.keySet().iterator();
//        while (it.hasNext()) {
//            String key = it.next();
//            Set<Integer> hashSet = hashMapSet.get(key);
//            Iterator<Integer> it2 = hashSet.iterator();
//            while (it2.hasNext()) {
//                Integer i = it2.next();
//                System.out.println(key + " " + i);
//            }
//        }
////        while (true);
//    }
//
//    public void testWrite() throws IOException, ClassNotFoundException {
//        System.err.println("writing hashmapset\n");
//        String chrs[] = {"chr1", "chr2"};
//        for (int i = 0; i < chrs.length; ++i) {
//            Set<Integer> posSet = new HashSet<Integer>();
//            for (int j = 0; j < 400; ++j) {
//                posSet.add(j);
//            }
//            hashMapSet.put(chrs[i], posSet);
//        }
//        System.err.println("Done!");
//        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("vcf.ser"));
//        oos.writeObject(hashMapSet);
//    }
//
//    public static void main(String args[]) {
//        if (args.length < 2) {
//            System.err.println("Usage: " + args[0] + " <VCF_file>");
//            System.exit(1);
//        }
//        String infile = args[1];
//        VcfLookup vcfLookup = new VcfLookup();
//        try {
//            vcfLookup.parseVcf(infile);
//            boolean write = true;
//            if (write) {
//                //vcfLookup.testWrite();
//            } else {
//                vcfLookup.testRead();
//            }
//        } catch (Exception ex) {
//            System.err.println("Error in main of VcfLookup, caused by " + ex.toString());
//        }
//
//    }
}
