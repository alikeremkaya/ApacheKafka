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
            // Yerel değişken yerine _consumer'a atama yapın!
            _consumer = new ConsumerBuilder<string, OrderCreatedEvent>(bus.GetConsumerConfig(BusConstants.OrderCreatedEventTopicGroupId))
                .SetValueDeserializer(new CustomValueDeSerializer<OrderCreatedEvent>())
                .Build();

            _consumer.Subscribe(BusConstants.OrderCreatedEventTopicName);
            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (_consumer == null)
                {
                    logger.LogError("Consumer is not initialized.");
                    continue; // return yerine döngüyü sürdürün!
                }

                try
                {
                    // Consume işlemine CancellationToken ekleyin
                    var consumeResult = _consumer.Consume(stoppingToken);

                    // Mesaj işleme kodu...
                    var orderCreatedEvent = consumeResult.Message.Value;
                    logger.LogInformation($"User ID: {orderCreatedEvent.UserId}, Order Code: {orderCreatedEvent.OrderCode}");
                }
                catch (ConsumeException ex)
                {
                    logger.LogError($"Consume error: {ex.Error.Reason}");
                }
                catch (OperationCanceledException)
                {
                    logger.LogInformation("Consumer stopped.");
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unexpected error");
                }

                await Task.Delay(10, stoppingToken);
            }
        }
    }
}
