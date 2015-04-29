package example;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchanumolu on 4/28/15.
 */

public class HighLevelConsumer {

    public static String className = new Throwable().getStackTrace()[1].getClassName();
    public static final Logger LOG = Logger.getLogger(className);

    private String topic;

    private String consumerGroupId ;

    private String zookeepers ;

    private int numPartitions;

    private Options opts;

    private  ConsumerConnector consumer;

    private ExecutorService executor;

    public static void main(String[] args) {
        HighLevelConsumer highLevelConsumer = new HighLevelConsumer();
        try {
            highLevelConsumer.init(args);
        }catch (Exception e){
            System.err.println(e.getLocalizedMessage());
            highLevelConsumer.printUsage();
            System.exit(-1);
        }

        highLevelConsumer.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(highLevelConsumer.zookeepers, highLevelConsumer.consumerGroupId));

        highLevelConsumer.run(highLevelConsumer.numPartitions);

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
        highLevelConsumer.shutdown();
    }

    public HighLevelConsumer(){
        LOG.info("Constructor for SimpleConsumer!!");

        opts = new Options();

        opts.addOption("topic",true,"Specify the topic for the consumer");

        opts.addOption("consumer-group",true,"Specify the consumer group for the consumer");

        opts.addOption("zookeepers",true,"Specify comma separated zookeepers list with ports. eg" +
                                   "zookeeper1:5181,zookeeper2:5181");

        opts.addOption("num-partitions",true,"Specify no. of partitions for the topic.Used for creating threads!!");

        opts.addOption("help", false,"PrintUsage" );


    }

    private boolean init(String[] args) throws org.apache.commons.cli.ParseException{

        CommandLine cliParser = new GnuParser().parse(opts,args);

        if(args.length==0){
            throw new IllegalArgumentException("No command line args specified for the High Level Consumer to " +
                    "initialize!!");
        }

        if(cliParser.hasOption("help")){
            printUsage();
            return false;
        }

        if(cliParser.hasOption("topic")){
            topic = cliParser.getOptionValue("topic");
            LOG.info("Topic for the consumer : "+topic);
        }else{
            throw new IllegalArgumentException("No topic specified for the consumer!!");
        }

        if(cliParser.hasOption("consumer-group")){
            consumerGroupId = cliParser.getOptionValue("consumer-group");
        }else{
            throw new IllegalArgumentException("No consumer group specified for the consumer");
        }

        if(cliParser.hasOption("zookeepers")){
            zookeepers = cliParser.getOptionValue("zookeepers");
        }else{
            throw new IllegalArgumentException("Please specify comma seperated zookeepers list with ports. eg" +
                    "zookeeper1:5181,zookeeper2:5181");
        }

        if(cliParser.hasOption("num-partitions")){
            numPartitions = Integer.parseInt(cliParser.getOptionValue("num-partitions"));
        }else{
            throw new IllegalArgumentException("No. of partitions not specified!!");
        }

        return true;
    }

    /**
     * zookeeper.connect -> helps to find the zookeeper in the cluster
     * group.id -> which group this consumer belongs to
     * zookeeper.session.timeout.ms -> zookeeper timeout
     * zookeeper.sync.time.ms -> number of milliseconds a ZooKeeper ‘follower’ can be behind the master before
     an error occurs.
       auto.commit.interval.ms -> how often updates to the consumed offsets are written to ZooKeeper
     */
    private static ConsumerConfig createConsumerConfig(String zookeepers, String consumerGroupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeepers);
        props.put("group.id", consumerGroupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("HighLevelConsumer", opts);
    }

    public void run(int numThreads){
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,numThreads);
        Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all threads
        executor = Executors.newFixedThreadPool(numThreads);

        // now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new RealConsumer(stream, threadNumber));
            threadNumber++;
        }

    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException ie) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        } catch(NullPointerException ne){
            System.out.println("executor is null!!");
        }
    }

}

class RealConsumer implements Runnable{

    private  KafkaStream stream;
    private int threadNum;

    public RealConsumer(KafkaStream stream, int threadNum) {
        this.stream = stream;
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) //The interesting part here is the while (it.hasNext()) section.
            // Basically this code reads from Kafka until you stop it.
            System.out.println("Thread " + threadNum + ": " + new String(it.next().message()));
        System.out.println("Shutting down Thread: " + threadNum);
    }
}