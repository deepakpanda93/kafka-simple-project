package example; /**
 * Created by pchanumolu on 4/28/15.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.util.Properties;

public class SimpleProducer {

    public static String className = new Throwable().getStackTrace()[1].getClassName();
    public static final Logger LOG = Logger.getLogger(className);

    private  String topic ;

    private  String brokers;

    private int numOfMessages;

    private String patitionIp = null;

    // Command line options
    private Options opts;

    public static void main(String[] args) {

        SimpleProducer simpleProducer = new SimpleProducer();
        try{
            simpleProducer.init(args);
        }catch (Exception e){
            System.err.println(e.getLocalizedMessage());
            simpleProducer.printUsage();
            System.exit(-1);
        }

        Properties props = simpleProducer.setProperties();

        ProducerConfig config = new ProducerConfig(props);
        Producer<String,String> producer = new Producer<String, String>(config);

        if(null != simpleProducer.patitionIp)
            for(int i=0;i<simpleProducer.numOfMessages;i++){
                KeyedMessage<String,String> msg = new KeyedMessage<String, String>(simpleProducer.topic,
                    simpleProducer.patitionIp,"sample-msg-"+i);
            }
        else
            for(int i=0;i<simpleProducer.numOfMessages;i++){
                KeyedMessage<String,String> msg = new KeyedMessage<String, String>(simpleProducer.topic,
                        "sample-msg-"+i);
            }

        LOG.info("Finished pubishing "+simpleProducer.numOfMessages+" messsges to the topic "+simpleProducer.topic);
        LOG.info("Producer shutting down");

    }

    public SimpleProducer(){
        opts = new Options();
        opts.addOption("topic", true,
                "Topic name to send messages by producer");

        opts.addOption("brokers",true,"comma separated list of brokers with ports. eg:broker1:12212,broker2:13323");

        opts.addOption("numMaps",false,"number of messages to send before turning down producer");

        opts.addOption("patitionIp",false,"Ip address to be used as partition key");

        opts.addOption("help", false,"PrintUsage" );
    }

    private boolean init(String[] args) throws org.apache.commons.cli.ParseException{
        CommandLine cliParser = new GnuParser().parse(opts,args);

        if(args.length==0){
            throw new IllegalArgumentException("No command line args specified for the Producer to initialize!!");
        }

        if(cliParser.hasOption("help")){
            printUsage();
            return false;
        }

        if(cliParser.hasOption("topic")){
            topic = cliParser.getOptionValue("topic","test");
            LOG.info("Topic: "+topic);
        }else{
            throw new IllegalArgumentException("No topic specified in the command line args. use --help to know all " +
                    "options");
        }

        if(cliParser.hasOption("brokers")){
            brokers = cliParser.getOptionValue("brokers","broker1:12231");
            LOG.info("Brokers: "+brokers);
        }else{
            throw new IllegalArgumentException("No commandline arg specified for brokers. use --help to know all " +
                    "options");
        }

        if(cliParser.hasOption("numMsgs")){
            numOfMessages = Integer.parseInt(cliParser.getOptionValue("numMsgs","1000"));
            LOG.info("Num of messages : "+ numOfMessages);
        }else{
            LOG.info("No. of messages not specified !! Using 1000 as default value!!");
            numOfMessages = 1000;
        }

        if(cliParser.hasOption("patitionIp")){
            patitionIp = cliParser.getOptionValue("patitionIp","0.0.0.0");
            LOG.info("Partition Ip : "+patitionIp);
        }

        return true;

    }

    /**
     * Set the properties for Producer
     */
    public Properties setProperties(){
        Properties props = new Properties();
        // set the brockers
        props.put("metadata.broker.list", brokers);
        // serializer class to serialize the message
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // set the partitioner
        props.put("partitioner.class", "example.SimplePartitioner");
        props.put("request.required.acks", "1");

        return props;
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Producer", opts);
    }
}
