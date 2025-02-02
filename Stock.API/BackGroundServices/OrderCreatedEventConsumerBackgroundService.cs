using Confluent.Kafka;
using Shared.Events;
using Shared.Events.Events;
using Stock.API.Services;

namespace Stock.API.BackGroundServices
{ 
    public class OrderCreatedEventConsumerBackgroundService(IBus bus,ILogger<OrderCreatedEventConsumerBackgroundService> logger) : BackgroundService
    {
        private IConsumer<string, OrderCreatedEvent>? _consumer;
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            var consumer = new ConsumerBuilder<string, OrderCreatedEvent>(bus.GetConsumerConfig(BusConstants.OrderCreatedEventTopicGroupId))
                .SetValueDeserializer(new CustomValueDeSerializer<OrderCreatedEvent>()).Build();

            consumer.Subscribe(BusConstants.OrderCreatedEventTopicName);
            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (_consumer == null)
                {
                    logger.LogError("Consumer is not initialized.");
                    return;
                }

                var consumeResult = _consumer.Consume(5000);
                if (consumeResult != null)
                {
                    try
                    {
                        var orderCreatedEvent = consumeResult.Message.Value;
                        // decrease stock count
                        logger.LogInformation($"user id:{orderCreatedEvent.UserId} order code: {orderCreatedEvent.OrderCode}, total price: {orderCreatedEvent.TotalPrice}");
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e.Message);
                    }
                    await Task.Delay(10, stoppingToken);
                }
            }
        }
    }
}
