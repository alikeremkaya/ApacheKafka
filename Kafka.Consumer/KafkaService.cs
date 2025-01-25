using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Consumer
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
                    Console.WriteLine($"gelen mesaj: Key={consumeResult.Message.Key} Value= {consumeResult.Message.Value}");
                }
               
            }
            await  Task.Delay(500);

            

        }

    }
}
