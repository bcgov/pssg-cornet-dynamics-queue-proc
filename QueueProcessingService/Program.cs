using NATS.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QueueProcessingService.Util;
using System;
using System.Net.Http;
using System.Threading;


namespace QueueProcessingService
{
    public class QueueProcess
    {
        //Dictionary<string, string> parsedArgs = new Dictionary<string, string>();
        int count = 0;
        int received = 0;
        // bool shutdown = false;

        // Environment Variable Configuration
        string url = ConfigurationManager.FetchConfig("QUEUE_URL");
        string subject = ConfigurationManager.FetchConfig("QUEUE_SUBJECT");
        //bool verbose = (ConfigurationManager.FetchConfig("VERBOSE") == "TRUE");
        string username = ConfigurationManager.FetchConfig("QUEUE_USERNAME");
        string password = ConfigurationManager.FetchConfig("QUEUE_PASSWORD");
        string queue_group = ConfigurationManager.FetchConfig("QUEUE_GROUP");
        public static int reconnect_attempts = 0;

        public static void Main(string[] args)
        {
            reconnect_attempts = Int32.TryParse(ConfigurationManager.FetchConfig("SERVER_RECONNECT_ATTEMPTS"), out int i) ? i : 0;
            AttemptLabel:
            try
            {
                new QueueProcess().Run(args);
            }
            catch (Exception ex)
            {
                System.Console.Error.WriteLine("Exception: " + ex.Message);
                System.Console.Error.WriteLine(ex);
                
                if (reconnect_attempts > 0)
                {
                    reconnect_attempts--;
                    goto AttemptLabel;
                }
            }
        }

        public void Run(string[] argsx)
        {
            banner();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;

            using (IConnection connection = new ConnectionFactory().CreateConnection(opts))
            {
                receiveAsyncSubscriber(connection);

                System.Console.Write("Received {0} msgs", received);
            }
        }

        private void receiveAsyncSubscriber(IConnection connection)
        {
            Object testLock = new Object();

            EventHandler<MsgHandlerEventArgs> msgHandler = (sender, args) =>
            {
                processMessage(args.Message);
                received++;

                Console.WriteLine("Received: " + args.Message);
            };

            using (IAsyncSubscription subscription = connection.SubscribeAsync(subject, queue_group, msgHandler))
            {
                // just wait until we are done.
                lock (testLock)
                {
                    Monitor.Wait(testLock);
                }
            }

            // Closing a connection
            connection.Flush(); //??
            connection.Close(); //??
        }

        private void processMessage(Msg natMessage)
        {
            String natMessageString = System.Text.Encoding.UTF8.GetString(natMessage.Data, 0, natMessage.Data.Length);

            Console.WriteLine("\n\n\n\n\n\n"); // Flush the Log a bit
            Console.WriteLine("Received: " + natMessageString);
            Console.WriteLine("\n"); // Flush the Log a bit

            HttpResponseMessage data;

            NatMessageObj natMessageObj = JsonConvert.DeserializeObject<NatMessageObj>(natMessageString);
            
            string msgVerb = natMessageObj.verb;
            string msgUrl = natMessageObj.requestUrl;
            string msgResponseUrl = natMessageObj.responseUrl;

            JRaw payload = natMessageObj.payload;

            Console.WriteLine(msgVerb + " FOR: " + msgUrl);

            switch (msgVerb)
            {
                case "POST":
                    data = DataClient.PostAsync(msgUrl, payload).Result;
                    break;
                case "GET":
                    data = DataClient.GetAsync(msgUrl).Result;
                    break;
                case "PUT":
                    data = DataClient.PutAsync(msgUrl, payload).Result;
                    break;
                case "DELETE":
                    data = DataClient.DeleteAsync(msgUrl, payload).Result;
                    break;
                default:
                    throw new Exception("Invalid VERB, message not processed");
            }

            if (data.IsSuccessStatusCode)
            {
                Console.WriteLine("Success Code: " + data.StatusCode);
                // @TODO Dequeue process here

                JRaw msgResponse = new JRaw(JsonConvert.DeserializeObject(data.Content.ReadAsStringAsync().Result));

                // Only return the results if response URL was set, some requests require no response
                if (!string.IsNullOrEmpty(msgResponseUrl))
                {
                    Console.WriteLine("Response Data: " + msgResponse);
                    Console.WriteLine("Sending Response data to: " + msgResponseUrl);

                    HttpResponseMessage responseData = DataClient.PostAsync(msgResponseUrl, msgResponse).Result;
                    // @TODO - Log success or failure here
                    if (responseData.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Final Response: " + new JRaw(JsonConvert.DeserializeObject(responseData.Content.ReadAsStringAsync().Result)));
                    }
                    else
                    {
                        Console.WriteLine("Failure Code: " + data.StatusCode);

                        // @TODO - Failure Retries
                    }
                }
                else
                {
                    Console.WriteLine("Response Data: " + msgResponse);
                    Console.WriteLine("No Response URL Set, work is complete ");
                }
            }
            else
            {
                Console.WriteLine("Error Code: " + data.StatusCode);
                // @TODO Error Escalation Strategy/Queue
            }
        }

        private void banner()
        {
            System.Console.WriteLine("Receiving {0} messages on subject {1}", count, subject);
            System.Console.WriteLine("  Url: {0}", url);
            System.Console.WriteLine("  Subject: {0}", subject);
        }    
    }

}


