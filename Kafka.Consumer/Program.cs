

using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
var topicName = "ack-topic";
var kafkaService = new KafkaService();
await kafkaService.ConsumeMessageWithAct(topicName);
Console.ReadLine();
