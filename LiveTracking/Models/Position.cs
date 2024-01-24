using Newtonsoft.Json;

namespace LiveTracking.Models;
public class Position
{
    [JsonProperty("type")]
    public string Type { get; set; }

    [JsonProperty("coordinates")]
    public double[] Coordinates { get; set; }
}