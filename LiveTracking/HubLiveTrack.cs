using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

//namespace Tracking.Vehicules
namespace LiveTracking
{
    public static class HubLiveTrack
    {
        [FunctionName("HubLiveTrack")]
        public static async Task Run(
            [EventHubTrigger("wbc", ConsumerGroup = "sink2", Connection = "HubConstr")] EventData[] events,
            [CosmosDB(
                databaseName: "tracking-db",
                containerName: "eventrack",
                Connection = "CosmosDBConn")] IAsyncCollector<EventData> outputItems,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    log.LogInformation($"Event Hub trigger function processed a message: {eventData.EventBody}");

                    // Add the event data to the Cosmos DB output
                    await outputItems.AddAsync(eventData);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}