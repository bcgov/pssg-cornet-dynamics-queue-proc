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
        int count = 0;
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
            banner();
            StanConnectionFactory stanConnectionFactory = new StanConnectionFactory();
            StanOptions stanOptions = StanOptions.GetDefaultOptions();
            stanOptions.NatsURL = String.Format("nats://{0}", url);
            using (IStanConnection c = stanConnectionFactory.CreateConnection(clusterName, clientID, stanOptions))
            {              
                receiveAsyncSubscriber(c);
            }
        }

        private void receiveAsyncSubscriber(IStanConnection c)
        {
            Object testLock = new Object();
            StanSubscriptionOptions sOpts = StanSubscriptionOptions.GetDefaultOptions();
            sOpts.DurableName = durableName;
            EventHandler<StanMsgHandlerArgs> msgHandler = (sender, args) =>
            {
                processMessage(args.Message);
            };

            using (IStanSubscription s = c.Subscribe(subject, sOpts, msgHandler))
            {
                // just wait until we are done.
                lock (testLock)
                {
                    Monitor.Wait(testLock);
                }
            }
        }
        private void processMessage(StanMsg m)
        {
            NatMessageObj natMessageObj = JsonConvert.DeserializeObject<NatMessageObj>(System.Text.Encoding.UTF8.GetString(m.Data, 0, m.Data.Length));
            Console.WriteLine(Environment.NewLine); // Flush the Log a bit
            Console.WriteLine("{0}: Received Event: {1}", DateTime.Now, natMessageObj.eventId);
            Console.WriteLine("{0}: Message: {1}", DateTime.Now, JsonConvert.SerializeObject(natMessageObj));
            HttpResponseMessage data = new HttpResponseMessage(); 
            HttpResponseMessage responseData = new HttpResponseMessage();
            String dynamicsRespStr;
            DynamicsResponse MsgResponse;
            QueueClient queueClient = new QueueClient(clusterName, ConfigurationManager.FetchConfig("RE_QUEUE_CLIENT_ID"));


            string MsgVerb = natMessageObj.verb;
            string MsgUrl = natMessageObj.requestUrl;
            string MsgResponseUrl = natMessageObj.responseUrl;

            JRaw payload = natMessageObj.payload;

            Console.WriteLine("    {0} FOR: {1}", MsgVerb, MsgUrl);

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

            Console.WriteLine("     Recieved Status Code: {0}", data.StatusCode);
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
                        Console.WriteLine("    Response Data: {0}", msgResStr);
                        Console.WriteLine("    Sending Response data to: {0}", MsgResponseUrl);

                        responseData = DataClient.PostAsync(MsgResponseUrl, JsonConvert.DeserializeObject<JRaw>(msgResStr)).Result;
                        if (responseData.IsSuccessStatusCode)
                        {
                            dynamicsRespStr = responseData.Content.ReadAsStringAsync().Result;
                            Console.WriteLine("    Adpater Response: {0}", dynamicsRespStr);

                            MsgResponse = JsonConvert.DeserializeObject<DynamicsResponse>(dynamicsRespStr);
                            Console.WriteLine("    Dynamics Status Code: {0}", MsgResponse.httpStatusCode);
                            //Handle successful dynamics response
                            if ((int)MsgResponse.httpStatusCode >= 200 && (int)MsgResponse.httpStatusCode <= 299)
                            {
                                Console.WriteLine("    EventId: {0} has succeeded. {1} Dynamics Response: {2}", natMessageObj.eventId, Environment.NewLine, dynamicsRespStr);
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
                        Console.WriteLine("    Response Data: {0}", msgResStr);
                        Console.WriteLine("    No Response URL Set, work is complete ");
                    }
                }
                else if (MsgVerb == "POST")
                {
                    Console.WriteLine("    Data has been posted succesfully to Cornet.");
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
                Console.WriteLine("    Error Code: {0}", failureStatusCode);
                natMessageObj.errorCount++;
                //Have we exceeded the maximum retry?
                if (natMessageObj.errorCount <= maxErrorRetry)
                {
                    Console.WriteLine("    EventId: {0} has failed at {1}. Error#: {2}. HttpStatusCode: {3}", natMessageObj.eventId, failureLocation, natMessageObj.errorCount, failureStatusCode);
                    //Re-queue
                    queueClient.QueueDynamicsNotficiation(natMessageObj);
                }
                else
                {
                    //TODO What do we do with a max error count?
                    Console.WriteLine("    EventId: {0} has failed at the {1}. No more attempts will be made. HttpStatusCode: {2}", natMessageObj.eventId, failureLocation, failureStatusCode);
                }
            }
            data.Dispose();
            responseData.Dispose();
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


