using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NATS.Client;
using Newtonsoft.Json;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using QueueProcessingService.Util;


namespace QueueProcessingService
{
    public class QueueProcess
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();
        int count = 0;
        int received = 0;
        bool shutdown = false;

        // Environment Variable Configuration
        string url = ConfigurationManager.FetchConfig("QUEUE_URL");
        string subject = ConfigurationManager.FetchConfig("QUEUE_SUBJECT");
        bool sync = (ConfigurationManager.FetchConfig("SYNCHRONOUS") == "TRUE");
        bool verbose = (ConfigurationManager.FetchConfig("VERBOSE") == "TRUE");
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

        public void Run(string[] args)
        {
            parseArgs(args);
            banner();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                TimeSpan elapsed;

                if (sync)
                {
                    elapsed = receiveSyncSubscriber(c);
                }
                else
                {
                    elapsed = receiveAsyncSubscriber(c);
                }

                System.Console.Write("Received {0} msgs in {1} seconds ", received, elapsed.TotalSeconds);
                System.Console.WriteLine("({0} msgs/second).",
                    (int)(received / elapsed.TotalSeconds));
                printStats(c);

            }
        }

        private void printStats(IConnection c)
        {
            IStatistics s = c.Stats;
            System.Console.WriteLine("Statistics:  ");
            System.Console.WriteLine("   Incoming Payload Bytes: {0}", s.InBytes);
            System.Console.WriteLine("   Incoming Messages: {0}", s.InMsgs);
        }

        private TimeSpan receiveAsyncSubscriber(IConnection c)
        {
            Stopwatch sw = new Stopwatch();
            Object testLock = new Object();

            EventHandler<MsgHandlerEventArgs> msgHandler = (sender, args) =>
            {
                if (received == 0)
                    sw.Start();

                processMessage(args.Message);

                received++;

                if (verbose)
                    Console.WriteLine("Received: " + args.Message);

               // if (received >= count)
               // {
               //     sw.Stop();
               //     lock (testLock)
               //     {
               //         Monitor.Pulse(testLock);
               //     }
               // }
            };

            using (IAsyncSubscription s = c.SubscribeAsync(subject, queue_group, msgHandler))
            {
                // just wait until we are done.
                lock (testLock)
                {
                    Monitor.Wait(testLock);
                }
            }

            return sw.Elapsed;
        }

        private void processMessage(Msg m)
        {
            Console.WriteLine("Received: " + System.Text.Encoding.UTF8.GetString(m.Data, 0, m.Data.Length));
            HttpResponseMessage data;

            NatMessageObj natMessageObj = JsonConvert.DeserializeObject<NatMessageObj>(System.Text.Encoding.UTF8.GetString(m.Data, 0, m.Data.Length));
            
            string MsgVerb = natMessageObj.verb;
            string MsgUrl = natMessageObj.requestUrl;
            string MsgResponseUrl = natMessageObj.responseUrl;

            JRaw payload = natMessageObj.payload;

            Console.WriteLine(MsgVerb + " FOR: " + MsgUrl);
            switch (MsgVerb)
            {
                case "POST":
                    data = DataClient.PostData(MsgUrl, payload);
                    break;
                case "GET":
                    Console.WriteLine("Inside the GET Case");
                    var Task = DataClient.GetAsync(MsgUrl);
                    data = Task.Result;
                    break;
                case "PUT":
                    data = DataClient.PutData(MsgUrl, payload);
                    break;
                case "DELETE":
                    data = DataClient.DeleteData(MsgUrl, payload);
                    break;
                default:
                    throw new Exception("Invalid VERB, message not processed");                    
            }

            if (data.IsSuccessStatusCode)
            {
                Console.WriteLine("Success Code: " + data.StatusCode);
                // @TODO Dequeue process here

                JRaw MsgResponse = new JRaw(JsonConvert.DeserializeObject(data.Content.ReadAsStringAsync().Result));
                
                Console.WriteLine("Response Data: " + MsgResponse);
                Console.WriteLine("Sending Response to: " + MsgResponseUrl);

                HttpResponseMessage responseData = DataClient.PostData(MsgResponseUrl, MsgResponse);
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
                Console.WriteLine("Error Code: " + data.StatusCode);
                // @TODO Error Escalation Strategy/Queue

            }
            
            

        }

        private TimeSpan receiveSyncSubscriber(IConnection c)
        {
            using (ISyncSubscription s = c.SubscribeSync(subject, queue_group))
            {
                Stopwatch sw = new Stopwatch();

               
                //if (received == 0)
                sw.Start();

                processMessage(s.NextMessage());
               
                received++;
                Console.WriteLine("Number of queued messages: " + s.QueuedMessageCount.ToString());
                int i = 0;

                while (i < 15)
                {
                    i++;
                    System.Threading.Thread.Sleep(500);
                    Console.Write("=");
                }


                sw.Stop();

                return sw.Elapsed;
            }
        }

        private void usage()
        {
            System.Console.Error.WriteLine(
                "Usage:  Subscribe [-url url] [-subject subject] " +
                "-count [count] [-sync] [-verbose]");

            System.Environment.Exit(-1);
        }

        private void parseArgs(string[] args)
        {
            if (args == null)
                return;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("-sync") ||
                    args[i].Equals("-verbose"))
                {
                    parsedArgs.Add(args[i], "true");
                }
                else
                {
                    if (i + 1 == args.Length)
                        usage();

                    parsedArgs.Add(args[i], args[i + 1]);
                    i++;
                }

            }

            if (parsedArgs.ContainsKey("-count"))
                count = Convert.ToInt32(parsedArgs["-count"]);

            if (parsedArgs.ContainsKey("-url"))
                url = parsedArgs["-url"];

            if (parsedArgs.ContainsKey("-subject"))
                subject = parsedArgs["-subject"];

            if (parsedArgs.ContainsKey("-sync"))
                sync = true;

            if (parsedArgs.ContainsKey("-verbose"))
                verbose = true;
        }

        private void banner()
        {
            System.Console.WriteLine("Receiving {0} messages on subject {1}",
                count, subject);
            System.Console.WriteLine("  Url: {0}", url);
            System.Console.WriteLine("  Subject: {0}", subject);
            System.Console.WriteLine("  Receiving: {0}",
                sync ? "Synchronously" : "Asynchronously");
        }

    
}



}


