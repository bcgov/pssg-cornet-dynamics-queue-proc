using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NATS.Client;
using Newtonsoft.Json;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using QueueProcessingService.Util;
using STAN.Client;
using Objects;
using QueueProcessingService.Client;
using System.Net;

namespace QueueProcessingService
{
    public class QueueProcess
    {
        Dictionary<string, string> parsedArgs = new Dictionary<string, string>();
        int count = 0;
        int received = 0;
        // bool shutdown = false;

        // Environment Variable Configuration
        string url = ConfigurationManager.FetchConfig("QUEUE_URL");
        string subject = ConfigurationManager.FetchConfig("QUEUE_SUBJECT");
        bool sync = (ConfigurationManager.FetchConfig("SYNCHRONOUS") == "TRUE");
        bool verbose = (ConfigurationManager.FetchConfig("VERBOSE") == "TRUE");
        string username = ConfigurationManager.FetchConfig("QUEUE_USERNAME");
        string password = ConfigurationManager.FetchConfig("QUEUE_PASSWORD");
        string queue_group = ConfigurationManager.FetchConfig("QUEUE_GROUP");
        string durableName = ConfigurationManager.FetchConfig("DURABLE_NAME");
        string clusterName = ConfigurationManager.FetchConfig("CLUSTER_NAME");
        string clientID = ConfigurationManager.FetchConfig("CLIENT_ID");
        int maxErrorRetry = int.Parse(ConfigurationManager.FetchConfig("MAX_RETRY").ToString());
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
            StanConnectionFactory stanConnectionFactory = new StanConnectionFactory();
            StanOptions stanOptions = StanOptions.GetDefaultOptions();
            stanOptions.NatsURL = String.Format("nats://{0}", url);
            using (var c = stanConnectionFactory.CreateConnection(clusterName, clientID, stanOptions))
            {
                TimeSpan elapsed;

                if (sync)
                {
                    elapsed = syncSubscriber(c);
                }
                else
                {
                    elapsed = receiveAsyncSubscriber(c);
                }

                System.Console.Write("Received {0} msgs in {1} seconds ", received, elapsed.TotalSeconds);
                System.Console.WriteLine("({0} msgs/second).",
                    (int)(received / elapsed.TotalSeconds));
                //printStats(c);

            }
        }

        private TimeSpan syncSubscriber(IStanConnection c)
        {
            Stopwatch sw = new Stopwatch();


            //if (received == 0)
            sw.Start();
            var opts = StanSubscriptionOptions.GetDefaultOptions();
            opts.DurableName = durableName;
            using (var s = c.Subscribe(subject, opts, (obj, args) =>
            {

                processMessage(args.Message);

            }))
            {
                sw.Stop();

                return sw.Elapsed;

            }

        }


        private void printStats(IConnection c)
        {
            IStatistics s = c.Stats;
            System.Console.WriteLine("Statistics:  ");
            System.Console.WriteLine("   Incoming Payload Bytes: {0}", s.InBytes);
            System.Console.WriteLine("   Incoming Messages: {0}", s.InMsgs);
        }
        private TimeSpan receiveAsyncSubscriber(IStanConnection c)
        {
            Stopwatch sw = new Stopwatch();
            Object testLock = new Object();
            StanSubscriptionOptions sOpts = StanSubscriptionOptions.GetDefaultOptions();
            sOpts.DurableName = durableName;
            EventHandler<StanMsgHandlerArgs> msgHandler = (sender, args) =>
            {
                if (received == 0)
                    sw.Start();

                processMessage(args.Message);

                received++;
            };

            using (var s = c.Subscribe(subject, sOpts, msgHandler))
            {
                // just wait until we are done.
                lock (testLock)
                {
                    Monitor.Wait(testLock);
                }
            }

            return sw.Elapsed;
        }
        private void processMessage(StanMsg m)
        {
            NatMessageObj natMessageObj = JsonConvert.DeserializeObject<NatMessageObj>(System.Text.Encoding.UTF8.GetString(m.Data, 0, m.Data.Length));
            Console.WriteLine(Environment.NewLine); // Flush the Log a bit
            Console.WriteLine(String.Format("Received Event: {0}", natMessageObj.eventId));
            Console.WriteLine(String.Format("Message: {0}", JsonConvert.SerializeObject(natMessageObj)));
            HttpResponseMessage data;
            QueueClient queueClient = new QueueClient();


            string MsgVerb = natMessageObj.verb;
            string MsgUrl = natMessageObj.requestUrl;
            string MsgResponseUrl = natMessageObj.responseUrl;

            JRaw payload = natMessageObj.payload;

            Console.WriteLine(String.Format("{0} FOR: {1}", MsgVerb, MsgUrl));

            switch (MsgVerb)
            {
                case "POST":
                    data = DataClient.PostAsync(MsgUrl, payload).Result;
                    break;
                case "GET":
                    data = DataClient.GetAsync(MsgUrl).Result;
                    break;
                case "PUT":
                    data = DataClient.PutAsync(MsgUrl, payload).Result;
                    break;
                case "DELETE":
                    data = DataClient.DeleteAsync(MsgUrl, payload).Result;
                    break;
                default:
                    throw new Exception("Invalid VERB, message not processed");
            }

            Console.WriteLine(String.Format("Recieved Status Code: {0}", data.StatusCode));
            bool failure = false;
            String failureLocation = "";
            HttpStatusCode failureStatusCode = data.StatusCode;
            //Handle adapter response
            if (data.IsSuccessStatusCode)
            {
                if (MsgVerb == "GET")
                {
                    String msgResStr = data.Content.ReadAsStringAsync().Result;
                    // Only return the results if response URL was set, some requests require no response
                    if (!string.IsNullOrEmpty(MsgResponseUrl))
                    {
                        Console.WriteLine(String.Format("Response Data: {0}", msgResStr));
                        Console.WriteLine(String.Format("Sending Response data to: {0}", MsgResponseUrl));

                        HttpResponseMessage responseData = DataClient.PostAsync(MsgResponseUrl, JsonConvert.DeserializeObject<JRaw>(msgResStr)).Result;
                        if (responseData.IsSuccessStatusCode)
                        {
                            String dynamicsRespStr = responseData.Content.ReadAsStringAsync().Result;
                            Console.WriteLine(String.Format("Adpater Response: {0}", dynamicsRespStr));

                            DynamicsResponse MsgResponse = JsonConvert.DeserializeObject<DynamicsResponse>(dynamicsRespStr);
                            Console.WriteLine(String.Format("Dynamics Status Code: {0}", MsgResponse.httpStatusCode));
                            //Handle successful dynamics response
                            if ((int)MsgResponse.httpStatusCode >= 200 && (int)MsgResponse.httpStatusCode <= 299)
                            {
                                Console.WriteLine("EventId: {0} has succeeded. {1} Dynamics Response: {2}", natMessageObj.eventId, Environment.NewLine, dynamicsRespStr);
                            }
                            else
                            {
                                failure = true;
                                failureLocation = "Dynamics";
                                failureStatusCode = MsgResponse.httpStatusCode;
                            }
                        }
                        else
                        {
                            failure = true;
                            failureLocation = "Adapter";
                            failureStatusCode = responseData.StatusCode;
                        }
                    }
                    else
                    {
                        Console.WriteLine(String.Format("Response Data: {0}", msgResStr));
                        Console.WriteLine("No Response URL Set, work is complete ");
                    }
                }
                else if (MsgVerb == "POST")
                {
                    Console.WriteLine(String.Format("Data has been posted succesfully to Cornet."));
                    Console.WriteLine(JsonConvert.SerializeObject(payload));
                }
            }
            else
            {
                failure = true;
                failureLocation = "Cornet";
                failureStatusCode = data.StatusCode;
            }

            //Hanlde a failure at any point.
            if (failure)
            {
                Console.WriteLine(String.Format("Error Code: {0}", failureStatusCode));
                natMessageObj.errorCount++;
                //Have we exceeded the maximum retry?
                if (natMessageObj.errorCount <= maxErrorRetry)
                {
                    Console.WriteLine("EventId: {0} has failed at {1}. Error#: {2}. HttpStatusCode: {3}", natMessageObj.eventId, failureLocation, natMessageObj.errorCount, failureStatusCode);
                    //Re-queue
                    queueClient.QueueDynamicsNotficiation(natMessageObj);
                }
                else
                {
                    //TODO What do we do with a max error count?
                    Console.WriteLine("EventId: {0} has failed at the {1}. No more attempts will be made. HttpStatusCode: {2}", natMessageObj.eventId, failureLocation, failureStatusCode);
                }
            }
        }
        private TimeSpan receiveSyncSubscriber(IConnection c)
        {
            using (ISyncSubscription s = c.SubscribeSync(subject, queue_group))
            {
                Stopwatch sw = new Stopwatch();


                //if (received == 0)
                sw.Start();

                // processMessage(s.NextMessage());

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


