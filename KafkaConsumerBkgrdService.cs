using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Hubtel.Kafka.Test
{
    internal class KafkaConsumerBkgrdService: BackgroundService
    {
        private ConsumerConfig _config;
        public KafkaConsumerBkgrdService(ConsumerConfig config)
        {
            _config = config;
        }
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task.Run(() => Start(stoppingToken));
            return Task.CompletedTask;
        }

        private void Start(CancellationToken stoppingToken)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            
            using (var c = new ConsumerBuilder<Ignore,string>(_config).Build())
            {
                c.Subscribe("Kafka-Topic-1");
                while (!stoppingToken.IsCancellationRequested)
                {
                    var cr = c.Consume(cts.Token);
                    Console.WriteLine($"Consumed message=>'{cr.Message.Value}' on topic2 '{cr.TopicPartitionOffset}'");
                }
            }
            throw new NotImplementedException();
        }
    }
}