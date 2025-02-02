// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer!");


var kafkaService = new KafkaService();
var topicName= "mycluster-topic";
await kafkaService.CreateTopicRetryWithClusterAsync(topicName);
//await kafkaService.SendMessageToCluster(topicName);



Console.WriteLine("Mesajlar gönderilmiştir.");