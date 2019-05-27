using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace QueueProcessingService
{
    public class NatMessageObj
    {
        [JsonProperty("request_url")]
        public String requestUrl { get; set; }
        [JsonProperty("response_url")]
        public String responseUrl { get; set; }
        [JsonProperty("event_id")]
        public String eventId { get; set; }
        [JsonProperty("error_queue")]
        public String errorQueue { get; set; }
        [JsonProperty("verb")]
        public String verb { get; set; }
        [JsonProperty("payload")]
        public JRaw payload { get; set; }
    }
}
