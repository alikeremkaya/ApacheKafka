
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
var topicName = "use-case-1.1-topic";
var kafkaService = new KafkaService();
await kafkaService.CunsumeSimpleMessageWithNullKey(topicName);
Console.ReadLine();
