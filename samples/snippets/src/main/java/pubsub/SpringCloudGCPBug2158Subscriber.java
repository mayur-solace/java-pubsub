package pubsub;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


//Bug > https://github.com/GoogleCloudPlatform/spring-cloud-gcp/issues/2158
public class SpringCloudGCPBug2158Subscriber {

  public static void main(String... args) throws Exception {
    // TODO(developer): Set the GOOGLE_APPLICATION_CREDENTIALS to point to the service account key
    //export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

    // TODO(developer): Replace these variables before running the sample.
    final String projectId = "your-project-id";
    final String topicId = "your-topic-id";

    //TODO: toggle between true/false for testing behaviour of Subscription with ordering and without ordering
    final boolean withOrderingEnabled = true;

    createTopic(projectId, topicId);
    if (withOrderingEnabled) {
      final String subscriptionIdWithOrdering = "mpatel-test-topic1-sub-ordered";
      createSubscription(projectId, topicId, subscriptionIdWithOrdering, true);
      subscribeAsync(projectId, subscriptionIdWithOrdering);
    } else {
      String subscriptionIdNoOrdering = "mpatel-test-topic1-no-ordering";
      createSubscription(projectId, topicId, subscriptionIdNoOrdering, false);
      subscribeAsync(projectId, subscriptionIdNoOrdering);
    }
  }

  public static void createTopic(String projectId, String topicId) throws IOException {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(projectId, topicId);
      Topic topic = topicAdminClient.createTopic(topicName);
      System.out.println("Created topic: " + topic.getName());
    } catch (AlreadyExistsException ae) {
      //ignore if topic already exists.
    }
  }

  public static void createSubscription(
      String projectId, String topicId, String subscriptionId, boolean enableMsgOrdering)
      throws IOException {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
      ProjectSubscriptionName subscriptionName =
          ProjectSubscriptionName.of(projectId, subscriptionId);

      Subscription subscription =
          subscriptionAdminClient.createSubscription(
              Subscription.newBuilder()
                  .setName(subscriptionName.toString())
                  .setTopic(topicName.toString())
                  // Set message ordering to true for ordered messages in the subscription.
                  .setEnableMessageOrdering(enableMsgOrdering)
                  .build());

      System.out.println("Created a subscription: " + subscription.getAllFields());
    } catch (AlreadyExistsException ae) {
      //ignore if topic already exists.
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
        Subscription subscription = subscriptionAdminClient.getSubscription(SubscriptionName.of(projectId, subscriptionId));
        System.out.println("Existing Subscription: " + subscription.getAllFields());
      }
    }
  }

  public static void subscribeAsync(String projectId, String subscriptionId) {
    ProjectSubscriptionName subscriptionName =
        ProjectSubscriptionName.of(projectId, subscriptionId);

    // Instantiate an asynchronous message receiver.
    MessageReceiver receiver =
        (PubsubMessage message, AckReplyConsumer consumer) -> {
          // Handle incoming message, then ack the received message.
          System.out.printf("[%s] Id: %s, OrderingKey: %s, Data: %s%n", LocalDateTime.now(),
              message.getMessageId(), message.getOrderingKey(), message.getData().toStringUtf8());
          consumer.ack();
        };

    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
      // Start the subscriber.
      subscriber.startAsync().awaitRunning();
      System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
      // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
      subscriber.awaitTerminated(5000, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      // Shut down the subscriber after 30s. Stop receiving messages.
      subscriber.stopAsync();
    }
  }
}
