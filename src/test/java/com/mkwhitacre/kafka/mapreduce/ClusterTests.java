package com.mkwhitacre.kafka.mapreduce;

import com.mkwhitacre.kafka.mapreduce.utils.KafkaBrokerTestHarness;
import junit.framework.TestSuite;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.Properties;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        
        
        })
public class ClusterTests {

    private static boolean runAsSuite;

    private static TemporaryFolder folder = new TemporaryFolder();
    private static MiniDFSCluster dfsCluster;
    private static KafkaBrokerTestHarness kafka;
    private static Configuration conf;
    private static FileSystem fs;

    @BeforeClass
    public static void startHarness() throws IOException {
        runAsSuite = true;
        setupFileSystem();
        startKafka();
    }

    @AfterClass
    public static void endSuite() throws Exception {
        stopKafka();
        dfsCluster.shutdown(true);
        folder.delete();
    }

    public static void startTest() throws Exception {
        if (!runAsSuite) {
            setupFileSystem();
            startKafka();
        }
    }

    public static void endTest() throws Exception {
        if (!runAsSuite) {
            stopKafka();
            dfsCluster.shutdown(true);
            folder.delete();
        }
    }

    private static void stopKafka() throws IOException {
        kafka.tearDown();
    }

    private static void startKafka() throws IOException {
        Properties props = new Properties();
        props.setProperty("auto.create.topics.enable", Boolean.FALSE.toString());

        kafka = new KafkaBrokerTestHarness(props);
        kafka.setUp();
    }

    private static void setupFileSystem() throws IOException {
        folder.create();

        Configuration tempConfig = new Configuration();
        tempConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, folder.getRoot().getAbsolutePath());

        dfsCluster = new MiniDFSCluster.Builder(tempConfig).build();

        // Wait for the cluster to be totally up
        dfsCluster.waitClusterUp();

        fs = dfsCluster.getFileSystem();

        conf = fs.getConf();

        // Run Map/Reduce tests in process.
        conf.set("mapreduce.jobtracker.address", "local");
    }

    public static Configuration getConf() {
        // Clone the configuration so it doesn't get modified for other tests.
        return new Configuration(conf);
    }
}
