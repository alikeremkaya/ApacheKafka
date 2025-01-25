using Confluent.Kafka;
using System.Text.Json;

namespace Kafka.Consumer
{
    internal class CustomValueDeSerializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonSerializer.Deserialize<T>(data);
        }
    }
}
