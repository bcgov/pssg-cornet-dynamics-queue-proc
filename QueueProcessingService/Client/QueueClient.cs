using Newtonsoft.Json;
using Objects;
using QueueProcessingService.Util;
using STAN.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace QueueProcessingService.Client
{
    public class QueueClient
    {
        private int retriesMax = 5;
        public HttpResponseMessage QueueDynamicsNotficiation(NatMessageObj natsMessage)
        {
            //Setup NATS Options
            StanConnectionFactory stanConnectionFactory = new StanConnectionFactory();
            StanOptions stanOptions = StanOptions.GetDefaultOptions();
            stanOptions.NatsURL = String.Format("nats://{0}", ConfigurationManager.FetchConfig("QUEUE_URL"));
            Console.WriteLine("Using URL " + stanOptions.NatsURL);
            //Connect and publish to the queue
            int i = 0;
            while (i < retriesMax)
            {
                try
                {
                    using (var c = stanConnectionFactory.CreateConnection("local", "re-queue-publisher-id", stanOptions))
                    {
                        c.Publish(ConfigurationManager.FetchConfig("QUEUE_SUBJECT"), Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(natsMessage)));
                    }
                    return new HttpResponseMessage();
                }
                catch (Exception e)
                {
                    Console.WriteLine(String.Format("Error in queue connection. Retry {0} of 5, Message: {1} ", (i + 1).ToString(), e.Message));
                }
                i++;
            }
            throw new Exception("Connection to NATS-Streaming has failed.");
        }
    }
}
