using Kafka.Producer.DTOs;
using Shared.Events;
using Shared.Events.Events;

namespace Order.API.Services
{
    public class OrderService(IBus bus)
    {
        public async Task<bool> Create(OrderCreateRequestDto request)
        {
            //save to database
            var orderCode = Guid.NewGuid().ToString();
            var orderCreatedEvent = new OrderCreatedEvent(orderCode, request.UserId, request.TotalPrice);
           return await  bus.Publish(orderCode, orderCreatedEvent, BusConstants.OrderCreatedEventTopicName);
        }
    }
}
