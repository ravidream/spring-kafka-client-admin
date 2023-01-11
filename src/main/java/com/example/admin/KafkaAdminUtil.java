package com.example.admin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;

@Component
public class KafkaAdminUtil {
	
	AdminClient adminClient = null;
	public KafkaAdminUtil() {
		Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminClient = AdminClient.create(properties);
	}


    public void listTopics() throws ExecutionException, InterruptedException {

        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);

    }


    public void describeTopics(final String topicName) throws ExecutionException, InterruptedException {
    	List<String> topicList = new ArrayList<>();
    	topicList.add(topicName);
        final DescribeTopicsResult describeTopic = adminClient.describeTopics(topicList);
        try {
            final TopicDescription topicDescription = describeTopic.values().get(topicName).get();
            System.out.println("Description of demo topic:" + topicDescription);

            if (topicDescription.partitions().size() != 1) {
                System.out.println("Topic has wrong number of partitions. Exiting.");
                System.exit(-1);
            }
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                System.out.println("Topic " + topicName + " does not exist.");
            }
        }

    }


    public void createTopic(final String topicName, final int partitions, final short replicationFactor) throws ExecutionException, InterruptedException {

        final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
		final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
    }


    public void deleteTopic(final String topicName) throws ExecutionException, InterruptedException {
    	List<String> topicList = new ArrayList<>();
    	topicList.add(topicName);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicList);
        deleteTopicsResult.all().get();

        try {
            DescribeTopicsResult demoTopic = adminClient.describeTopics(topicList);
            TopicDescription topicDescription = demoTopic.values().get(topicName).get();
            System.out.println("Topic " + topicName + " is still present");
        } catch (ExecutionException e) {
            System.out.println("Topic " + topicName + " is deleted");
        }
    }
}
