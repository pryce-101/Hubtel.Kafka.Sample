using System;
using System.Threading;
using Confluent.Kafka;

namespace KafkaConsumerClient
{
    class Program
    {
       
        
        static void Main(string[] args)
        {
            var cConfig= new ConsumerConfig
            {
                BootstrapServers = "http://localhost:9092",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "admin",
                SaslPassword = "admin+",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var c = new ConsumerBuilder<Ignore, string>(cConfig).Build())
            {
                c.Subscribe("Kafka-Topic-1");
 
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };
 
                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Close and Release all the resources held by this consumer  
                    c.Close();
                }
            }
        }
        }
    }