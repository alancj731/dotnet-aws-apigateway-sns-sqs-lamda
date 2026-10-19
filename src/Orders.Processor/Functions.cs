using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Orders.Contracts;

// Tells Lambda how to turn the incoming JSON event into SQSEvent
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Orders.Processor;

public class Function
{
    public SQSBatchResponse FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        var failures = new List<SQSBatchResponse.BatchItemFailure>();

        foreach (var message in sqsEvent.Records)
        {
            try
            {
                var order = JsonSerializer.Deserialize<OrderCreated>(message.Body)
                    ?? throw new InvalidOperationException("Empty message");

                context.Logger.LogInformation($"Processing order {order.OrderId} for {order.CustomerEmail}, total {order.Total}");
            }
            catch (Exception ex)
            {
                context.Logger.LogError($"Failed to process message {message.MessageId}: {ex.Message}");
                failures.Add(new SQSBatchResponse.BatchItemFailure { ItemIdentifier = message.MessageId });
            }
        }

        // Only the failed messages go back to the queue to be retried
        return new SQSBatchResponse(failures);
    }
}