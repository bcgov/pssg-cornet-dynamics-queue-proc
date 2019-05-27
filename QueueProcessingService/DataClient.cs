using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace QueueProcessingService
{
    static class DataClient
    {
        public static HttpResponseMessage PostData(String endpoint, JRaw data)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                String jsonRequest = JsonConvert.SerializeObject(data);                
                HttpResponseMessage httpResponseMessage = httpClient.PostAsJsonAsync(endpoint, data).Result;
                return httpResponseMessage;
            }
        }

       

        public static HttpResponseMessage PutData(String endpoint, JRaw data)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                String jsonRequest = JsonConvert.SerializeObject(data);
                HttpResponseMessage httpResponseMessage = httpClient.PutAsJsonAsync(endpoint, data).Result;
                return httpResponseMessage;
            }
        }

        public static HttpResponseMessage DeleteData(String endpoint, JRaw data)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
                HttpResponseMessage httpResponseMessage = httpClient.DeleteAsync(endpoint).Result;
                return httpResponseMessage;
            }
        }

        public static HttpResponseMessage GetData(String endpoint)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
                HttpResponseMessage httpResponseMessage = httpClient.GetAsync(endpoint).Result;
                return httpResponseMessage;
            }
        }
    }
}
