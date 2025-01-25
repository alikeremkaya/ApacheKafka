

using Confluent.Kafka;

namespace Kafka.Consumer2
{
    internal class KafkaService
    {
        internal  async Task CunsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);



            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout:5000);
                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj: {consumeResult.Message.Value}");
                }
               
            }
            await  Task.Delay(500);

            

        }

    }
}
