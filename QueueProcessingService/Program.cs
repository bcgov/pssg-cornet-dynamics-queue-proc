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
using QueueProcessingService.Service;

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
        string clientID = ConfigurationManager.FetchConfig("CLIENT_ID");
        string clusterName = ConfigurationManager.FetchConfig("CLUSTER_NAME");
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
                //receiveSyncSubscriber(c);
                receiveAsyncSubscriber(c);
            }
        }

        private void receiveAsyncSubscriber(IStanConnection c)
        {
            AutoResetEvent ev = new AutoResetEvent(false);
            StanSubscriptionOptions sOpts = StanSubscriptionOptions.GetDefaultOptions();
            sOpts.DurableName = durableName;
            sOpts.ManualAcks = true;
            sOpts.AckWait = 60000;
            EventHandler<StanMsgHandlerArgs> msgHandler = (sender, args) =>
            {
                args.Message.Ack();
                using (MessageService messageService = new MessageService())
                {
                    messageService.processMessage(args.Message);
                }
            };

            using (IStanSubscription s = c.Subscribe(subject, sOpts, msgHandler))
            {
                // just wait until we are done.
                ev.WaitOne();
            }
        }
        private void receiveSyncSubscriber(IStanConnection c)
        {
        
            StanSubscriptionOptions sOpts = StanSubscriptionOptions.GetDefaultOptions();
            sOpts.DurableName = durableName;
            sOpts.ManualAcks = true;
            sOpts.AckWait = 60000;
            IStanSubscription s = c.Subscribe(subject, sOpts, (sender, args) =>
            {
                // just wait until we are done.
                args.Message.Ack();
                using (MessageService messageService = new MessageService())
                {
                    messageService.processMessage(args.Message);
                }
            });
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


