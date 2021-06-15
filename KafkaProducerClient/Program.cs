using System;
using System.Threading;
using Confluent.Kafka;

namespace KafkaProducerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var pConfig = new ProducerConfig
            {
                BootstrapServers = "http://localhost:9092",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "admin",
                SaslPassword = "admin"
            };

            Action<DeliveryReport<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivery message to {r.TopicPartitionOffset}"
                    : $"Delivery Error: {r.Error.Reason}");
            
            using (var p = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                for (int i = 0; i < 100; ++i)
                {
                  
                    p.Produce("Kafka-Topic-1", new Message<Null, string> { Value = "Test"+ i.ToString() }, handler);
                    Thread.Sleep(3000);
 
                }
 
                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
            
        }
        
        
        
    }
}