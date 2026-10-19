using System.Text.Json;
using Amazon.SimpleNotificationService;
using Orders.Contracts;

var builder = WebApplication.CreateBuilder(args);

// In Lambda, API Gateway (HTTP API) events are translated into normal HTTP requests.
// Outside Lambda this does nothing and the app runs on Kestrel as usual.
builder.Services.AddAWSLambdaHosting(LambdaEventSource.HttpApi);

builder.Services.AddDefaultAWSOptions(builder.Configuration.GetAWSOptions());
builder.Services.AddAWSService<IAmazonSimpleNotificationService>();

var app = builder.Build();

app.MapPost("/orders", async (CreateOrderRequest request, IAmazonSimpleNotificationService sns, IConfiguration config) =>
{
    // Set by Aspire locally (from the CloudFormation output in Step 4), and by a Lambda environment variable in AWS
    var topicArn = config["AWS:Resources:OrdersTopicArn"]
        ?? throw new InvalidOperationException("AWS:Resources:OrdersTopicArn is not configured");

    var order = new OrderCreated(Guid.NewGuid(), request.CustomerEmail, request.Total, DateTime.UtcNow);
    await sns.PublishAsync(topicArn, JsonSerializer.Serialize(order));

    // 202 Accepted: the order is queued, not processed yet
    return Results.Accepted($"/orders/{order.OrderId}", new { order.OrderId });
});

app.Run();

record CreateOrderRequest(string CustomerEmail, decimal Total);