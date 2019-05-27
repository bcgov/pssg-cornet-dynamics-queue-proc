using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client;

namespace QueueProcessingService
{
    public class Startup
    {

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IApplicationLifetime applicationLifetime)
        {
      
            applicationLifetime.ApplicationStopping.Register(OnShutdown);
           // if (env.IsDevelopment())
           // {
               // app.UseDeveloperExceptionPage();
           // }

            //app.Run(async (context) =>
            //{
                //await context.Response.WriteAsync("Hello World!");
            //});
        }

        public void OnShutdown()
        {
            string url = (Environment.GetEnvironmentVariable("QUEUE_URL") != null) ? Environment.GetEnvironmentVariable("QUEUE_URL") : Defaults.Url;
            string subject = (Environment.GetEnvironmentVariable("QUEUE_SUBJECT") != null) ? Environment.GetEnvironmentVariable("QUEUE_SUBJECT") : "Cornet.Dynamics";
            bool sync = (Environment.GetEnvironmentVariable("SYNCHRONOUS") != null) ? (Environment.GetEnvironmentVariable("SYNCHRONOUS") == "true") : false;
            bool verbose = (Environment.GetEnvironmentVariable("VERBOSE") != null) ? (Environment.GetEnvironmentVariable("VERBOSE") == "true") : true;
            string username = (Environment.GetEnvironmentVariable("QUEUE_USERNAME") != null) ? Environment.GetEnvironmentVariable("QUEUE_USERNAME") : "";
            string password = (Environment.GetEnvironmentVariable("QUEUE_PASSWORD") != null) ? Environment.GetEnvironmentVariable("QUEUE_PASSWORD") : "";

            //Options opts = ConnectionFactory.GetDefaultOptions();
            //opts.Url = url;

            //using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            //{
            //    TimeSpan elapsed;

            //    if (sync)
            //    {
            //        elapsed = receiveSyncSubscriber(c);
            //    }
            //    else
            //    {
            //        elapsed = receiveAsyncSubscriber(c);
            //    }

            //    System.Console.Write("Received {0} msgs in {1} seconds ", received, elapsed.TotalSeconds);
            //    System.Console.WriteLine("({0} msgs/second).",
            //        (int)(received / elapsed.TotalSeconds));
            //    printStats(c);

            //}

            //Wait while unsubscribe happens
            System.Threading.Thread.Sleep(1000);
        }
    }
}
