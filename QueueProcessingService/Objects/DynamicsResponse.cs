using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Objects
{
    public class DynamicsResponse
    {
        [JsonProperty("http_status")]
        public HttpStatusCode httpStatusCode { get; set; }
        [JsonProperty("dynamics_payload")]
        public JRaw dynamicsPayload { get; set; }
        [JsonProperty("dynamics_response")]
        public string dynamicsResponse { get; set; }
    }
}
