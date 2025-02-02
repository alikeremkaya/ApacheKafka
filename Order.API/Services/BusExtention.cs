using Shared.Events;

namespace Order.API.Services
{
    public static  class BusExtention
    {
        public static async Task CreateTopicsOrQueues(this WebApplication app)
        {
            using (var scope = app.Services.CreateScope())
            {
                var Ibus = scope.ServiceProvider.GetRequiredService<IBus>();
                await Ibus.CreateTopicOrQueue([BusConstants.OrderCreatedEventTopicName]);
            }
        }
    }
}
