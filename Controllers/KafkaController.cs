using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Hubtel.Kafka.Test.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class KafkaController : ControllerBase
    {
        private ProducerConfig _config;
        public KafkaController(ProducerConfig config)
        {
            this._config = config;
        }

        public string Get()
        {
            return "Api Ready";
        }

        [HttpPost("send")]
        public async Task<ActionResult> Send(string topic, [FromBody]NotificationModel notification)
        {
            string serializedEmployee = JsonConvert.SerializeObject(notification);
            using (var producer = new ProducerBuilder<Null, string>(_config).Build())
            {
                await producer.ProduceAsync(topic, new Message<Null, string> { Value = serializedEmployee });
                producer.Flush(TimeSpan.FromSeconds(10));
                return Ok(true);
            }
        }
    }

    public class NotificationModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Message { get; set; }
    }
}
