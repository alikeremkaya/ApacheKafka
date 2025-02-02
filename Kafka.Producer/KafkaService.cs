using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Producer.Events;

namespace Kafka.Producer;

internal class KafkaService
{

    internal async Task CreateTopicAsync(string topicName)
    {


        using var adminClient = new AdminClientBuilder(new AdminClientConfig()

        {

            BootstrapServers = "localhost:9094"

        }).Build();
        try
        {


            await adminClient.CreateTopicsAsync(new[]
            {
        new TopicSpecification(){Name=topicName, NumPartitions=6,ReplicationFactor=1 }
    });
            Console.WriteLine($"Topic({topicName}) oluştu.");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
    internal async Task CreateTopicWithRetentionAsync(string topicName)
    {


        using var adminClient = new AdminClientBuilder(new AdminClientConfig()

        {

            BootstrapServers = "localhost:9094"

        }).Build();
        try
        {
            TimeSpan day30Span = TimeSpan.FromSeconds(30);
            var configs = new Dictionary<string, string>
            {
                //{"retention.ms","3600000" }    
                {"retention.ms", day30Span.TotalMicroseconds.ToString() }
            };

            await adminClient.CreateTopicsAsync(new[]
            {
        new TopicSpecification(){Name=topicName, NumPartitions=6,ReplicationFactor=1, Configs=configs, }
    });
            Console.WriteLine($"Topic({topicName}) oluştu.");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
    internal async Task CreateTopicWithClusterAsync(string topicName)
    {


        using var adminClient = new AdminClientBuilder(new AdminClientConfig()

        {

            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",

        }).Build();
        try
        {
            TimeSpan day30Span = TimeSpan.FromSeconds(30);
            var configs = new Dictionary<string, string>
            {
                //{"retention.ms","3600000" }    
                {"retention.ms", day30Span.TotalMicroseconds.ToString() }
            };

            await adminClient.CreateTopicsAsync(new[]
            {
        new TopicSpecification(){Name=topicName, NumPartitions=6,ReplicationFactor=3,  }
    });
            Console.WriteLine($"Topic({topicName}) oluştu.");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
    internal async Task CreateTopicRetryWithClusterAsync(string topicName)

    {

        // we use using to dispose the resources after the task is done

        using var adminClient = new AdminClientBuilder(new AdminClientConfig()

        {

            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002" // This code is for local Kafka. 

        }).Build();

        try

        {

            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var topicExists = metadata.Topics.Any(m => m.Topic == topicName);

            // We can set the configuration for the topic. In this example, we set the message.timestamp.type to LogAppendTime. LogAppendTime is the time when the message is appended to the log.

            if (!topicExists)

            {

                var configs = new Dictionary<string, string>()

                {

                {"min.insync.replicas","3"}

            };

                await adminClient.CreateTopicsAsync(new[]

                {

                new TopicSpecification(){ Name= topicName, NumPartitions = 6, ReplicationFactor = 3, Configs=configs}

            }); // 3 partitions and 1 replication factor for the topic

                Console.WriteLine($"Topic({topicName}) is created.");

            }

            else

            {

                Console.WriteLine($"Topic({topicName}) already exists.");

            }

        }

        catch (Exception e)

        {

            Console.WriteLine(e.Message);

        }

    }



    internal async Task SendSimpleMessageWithNullKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 10))
        {
            var message = new Message<Null, string>()
            {
                Value = $"Message(use case -1) {item}"
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");


            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(200);

        }




    }

    internal async Task SendSimpleMessageWithIntKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<int, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 100))
        {
            var message = new Message<int, string>()
            {
                Value = $"Message(use case -1) {item}",
                Key = item
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");


            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);

        }




    }
    internal async Task SendComplexMessageWithIntKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config).
            SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).Build();



        foreach (var item in Enumerable.Range(1, 100))
        {
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item * 100,
                UserId = 1
            };




            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = item
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");


            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);

        }




    }
    internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).Build();

        foreach (var item in Enumerable.Range(1, 3))
        {
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item * 100,
                UserId = 1
            };
            var header = new Headers
            {
            {"correlation_id", Encoding.UTF8.GetBytes("123") },
            { "version", Encoding.UTF8.GetBytes("v1") }
        };


            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = item,
                Headers = header
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);
        }
    }


    internal async Task SendComplexMessageWithComplexKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).
            SetKeySerializer(new CustomKeySerializer<MessageKey>()).Build();


        foreach (var item in Enumerable.Range(1, 3))
        {
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item * 100,
                UserId = 1
            };



            var message = new Message<MessageKey, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = new MessageKey("key1 value", "key2 value")

            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageWithTimestamp(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).
            SetKeySerializer(new CustomKeySerializer<MessageKey>()).Build();


        foreach (var item in Enumerable.Range(1, 3))
        {
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item * 100,
                UserId = 1
            };



            var message = new Message<MessageKey, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = new MessageKey("key1 value", "key2 value"),
                Timestamp = new Timestamp(DateTimeOffset.UtcNow)

            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);
        }
    }

    internal async Task SendMessageWithToSpecificPartition(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<Null, string>(config)
           .Build();


        foreach (var item in Enumerable.Range(1, 30))
        {
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item * 100,
                UserId = 1
            };

            var topicPartition = new TopicPartition(topicName, new Partition(2));



            var message = new Message<Null, string>()
            {
                Value = $"Mesaj{item}"
            };

            var result = await producer.ProduceAsync(topicPartition, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);
        }
    }

    internal async Task SendMessageWithAck(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094",
            Acks = Acks.None
        };

        using var producer = new ProducerBuilder<Null, string>(config)
           .Build();


        foreach (var item in Enumerable.Range(1, 30))
        {
            var orderCreatedEvent = new OrderCreatedEvent()
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item * 100,
                UserId = 1
            };





            var message = new Message<Null, string>()
            {
                Value = $"Mesaj{item}"
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name}:{propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("----------------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageToCluster(string topicName)
    {

        var config = new ProducerConfig() { BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", Acks = Acks.All };


        using var producer = new ProducerBuilder<Null, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 20))
        {
            var message = new Message<Null, string> { Value = $"Message {item}" };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageWithRetryToCluster(string topicName)
    {

        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
            Acks = Acks.All,
            RetryBackoffMs = 2000,
            RetryBackoffMaxMs = 2000,
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var message = new Message<Null, string>()
        {
            Value = $"Message 1"
        };

        var result = await producer.ProduceAsync(topicName, message);

        foreach (var propertyInfo in result.GetType().GetProperties())
        {
            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
        }

        Console.WriteLine("---------------------------------");
    }
}
