using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QueueProcessingService.Util;
using System.Threading.Tasks;

namespace QueueProcessingService
{
    static class DataClient
    {
        private static int timeout = int.Parse(ConfigurationManager.FetchConfig("Request_Timeout").ToString());
        public static HttpResponseMessage PostData(String endpoint, JRaw data)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                String jsonRequest = JsonConvert.SerializeObject(data);
                HttpResponseMessage httpResponseMessage = httpClient.PostAsJsonAsync(endpoint, data).Result;
                return httpResponseMessage;
            }
        }

       

        public static HttpResponseMessage PutData(String endpoint, JRaw data)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                String jsonRequest = JsonConvert.SerializeObject(data);
                HttpResponseMessage httpResponseMessage = httpClient.PutAsJsonAsync(endpoint, data).Result;
                return httpResponseMessage;
            }
        }

        public static HttpResponseMessage DeleteData(String endpoint, JRaw data)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                //httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
                HttpResponseMessage httpResponseMessage = httpClient.DeleteAsync(endpoint).Result;
                return httpResponseMessage;
            }
        }

        

        public static async Task<HttpResponseMessage> GetAsync(string uri)
        {
            var httpClient = new HttpClient();
            HttpResponseMessage content = await httpClient.GetAsync(uri);
            return await Task.Run(() => content);
        }

        public static HttpResponseMessage GetData(String endpoint)
        {
            Console.WriteLine("Right before the httpClient Init");
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.Timeout = new TimeSpan(0, timeout, 0);
                    //httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
                    // Console.WriteLine("Performing a GET of " + endpoint);
                    HttpResponseMessage httpResponseMessage = httpClient.GetAsync(endpoint).Result;
             
                  //  Console.WriteLine("FINISHED!");
                    return httpResponseMessage;
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
                Console.WriteLine(Ex.InnerException.Message);
                Console.WriteLine(Ex.InnerException.InnerException.Message);
                return null;
            }
        }
    }
}
