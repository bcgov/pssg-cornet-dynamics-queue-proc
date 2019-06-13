using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QueueProcessingService.Util;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace QueueProcessingService
{
    static class DataClient
    {
        private static int timeout = int.Parse(ConfigurationManager.FetchConfig("Request_Timeout").ToString());   

        public static async Task<HttpResponseMessage> PostAsync(string uri, JRaw data)
        {
            try
            {
                HttpClient httpClient = new HttpClient();
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                HttpResponseMessage content = await httpClient.PostAsJsonAsync(uri,data);
                return await Task.Run(() => content);
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
                Console.WriteLine(Ex.InnerException.Message);
                Console.WriteLine(Ex.InnerException.InnerException.Message);
                return null;
            }
        }

        public static async Task<HttpResponseMessage> PutAsync(String endpoint, JRaw data)
        {
            try
            {
                HttpClient httpClient = new HttpClient();
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                String jsonRequest = JsonConvert.SerializeObject(data);
                HttpResponseMessage content = httpClient.PutAsJsonAsync(endpoint, data).Result;
                return await Task.Run(() => content);
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
                Console.WriteLine(Ex.InnerException.Message);
                Console.WriteLine(Ex.InnerException.InnerException.Message);
                return null;
            }
        }

        public static async Task<HttpResponseMessage> DeleteAsync(String endpoint, JRaw data)
        {
            try
            {
                HttpClient httpClient = new HttpClient();
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                //httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
                HttpResponseMessage content = httpClient.DeleteAsync(endpoint).Result;
                return await Task.Run(() => content);
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
                Console.WriteLine(Ex.InnerException.Message);
                Console.WriteLine(Ex.InnerException.InnerException.Message);
                return null;
            }
        }        

        public static async Task<HttpResponseMessage> GetAsync(string uri)
        {
            try
            {
                HttpClient httpClient = new HttpClient();
                httpClient.Timeout = new TimeSpan(0, timeout, 0);
                HttpResponseMessage content = await httpClient.GetAsync(uri);
                return await Task.Run(() => content);
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
