/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pubsub;

// [START pubsub_publish_with_ordering_keys]

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SpringCloudGCPBug2158Publisher {

  public static void main(String... args) throws Exception {
    // TODO(developer): Set the GOOGLE_APPLICATION_CREDENTIALS to point to the service account key
    //Note: service account key is different from the subscriber's service account key
    //You could also use ADC instead of service account key for publisher
    //export GOOGLE_APPLICATION_CREDENTIALS=/path/to/publisher-service-account-key.json

    // TODO(developer): Replace these variables before running the sample.
    final String projectId = "your-project-id";
    final String topicId = "your-topic-id";

    createTopic(projectId, topicId);
    publishWithOrderingKeysExample(projectId, topicId);
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

  public static void publishWithOrderingKeysExample(String projectId, String topicId)
      throws IOException, InterruptedException {
    TopicName topicName = TopicName.of(projectId, topicId);
    // Create a publisher and set message ordering to true.
    Publisher publisher =
        Publisher.newBuilder(topicName)
            // Sending messages to the same region ensures they are received in order
            // even when multiple publishers are used.
            .setEndpoint("us-east1-pubsub.googleapis.com:443")
            .setEnableMessageOrdering(true)
            .build();

    try {
      for (int i = 0; i < 1000; i++) {
        Map<String, String> messages = new LinkedHashMap<String, String>();
        messages.put("message1", "key" + i);
        messages.put("message2", "key" + i);
        messages.put("message3", "key" + i);

        for (Map.Entry<String, String> entry : messages.entrySet()) {
          ByteString data = ByteString.copyFromUtf8(entry.getKey());
          PubsubMessage pubsubMessage =
              PubsubMessage.newBuilder().setData(data).setOrderingKey(entry.getValue()).build();
          ApiFuture<String> future = publisher.publish(pubsubMessage);

          // Add an asynchronous callback to handle publish success / failure.
          ApiFutures.addCallback(
              future,
              new ApiFutureCallback<String>() {

                @Override
                public void onFailure(Throwable throwable) {
                  if (throwable instanceof ApiException) {
                    ApiException apiException = ((ApiException) throwable);
                    // Details on the API exception.
                    System.out.println(apiException.getStatusCode().getCode());
                    System.out.println(apiException.isRetryable());
                  }
                  System.out.println("Error publishing message : " + pubsubMessage.getData());
                }

                @Override
                public void onSuccess(String messageId) {
                  // Once published, returns server-assigned message ids (unique within the topic).
                  //System.out.println(pubsubMessage.getData() + " : " + messageId);

                  System.out.printf("[%s] Id: %s, OrderingKey: %s, Data: %s%n", LocalDateTime.now(),
                      pubsubMessage.getMessageId(), pubsubMessage.getOrderingKey(), pubsubMessage.getData().toStringUtf8());
                }
              },
              MoreExecutors.directExecutor());
        }

        //TODO: You may want to increase the delay
        Thread.sleep(2000); //Delay by 2 second
      }
    } finally {
      // When finished with the publisher, shutdown to free up resources.
      publisher.shutdown();
      publisher.awaitTermination(1, TimeUnit.MINUTES);
    }
  }
}
// [END pubsub_publish_with_ordering_keys]
