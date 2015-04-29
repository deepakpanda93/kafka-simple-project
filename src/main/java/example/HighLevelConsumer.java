package example;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

/**
 * Created by pchanumolu on 4/28/15.
 */

public class HighLevelConsumer {

    public static String className = new Throwable().getStackTrace()[1].getClassName();
    public static final Logger LOG = Logger.getLogger(className);

    private String topic;

    private String consumerGroup ;

    private String zookeepers ;

    private Options opts;

    public static void main(String[] args) {

    }

    public HighLevelConsumer(){
        LOG.info("Constructor for SimpleConsumer!!");

        opts = new Options();

        opts.addOption("topic",true,"Specify the topic for the consumer");

        opts.addOption("consumer-group",true,"Specify the consumer group for the consumer");

        opts.addOption("zookeepers",true,"Specify comma separated zookeepers list with ports. eg" +
                                   "zookeeper1:5181,zookeeper2:5181");


    }

    private boolean init(String[] args) throws org.apache.commons.cli.ParseException{

        CommandLine cliParser = new GnuParser().parse(opts,args);

        if(cliParser.hasOption("topic")){
            topic = cliParser.getOptionValue("topic");
            LOG.info("Topic for the consumer : "+topic);
        }else{
            throw new IllegalArgumentException("No topic specified for the consumer!!");
        }

        if(cliParser.hasOption("consumer-group")){
            consumerGroup = cliParser.getOptionValue("consumer-group");
        }else{
            throw new IllegalArgumentException("No consumer group specified for the consumer");
        }

        if(cliParser.hasOption("zookeepers")){
            zookeepers = cliParser.getOptionValue("zookeepers");
        }else{
            throw new IllegalArgumentException("Please specify comma seperated zookeepers list with ports. eg" +
                    "zookeeper1:5181,zookeeper2:5181");
        }

        return true;
    }
}
