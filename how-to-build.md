# How to build: Serverless .NET orders app (API Gateway → Lambda → SNS → SQS → Lambda)

Based on the video *"Serverless .NET: API Gateway → Lambda → SNS/SQS (End-to-End)"* by Milan Jovanović.

```
client ─► API Gateway ─► Orders.Api (Lambda) ──publish──► SNS topic ──► SQS queue ──trigger──► Orders.Processor (Lambda)
```

- **Orders.Api**: accepts `POST /orders`, publishes an `OrderCreated` event to SNS, and returns right away.
- **SNS topic**: fans the event out. Today there's one subscriber (the queue); later you can add more (email, analytics…).
- **SQS queue**: holds the event until the processor is ready, and retries it if processing fails.
- **Orders.Processor**: triggered by SQS, does the slow work (here: logs the order).
- **Aspire**: runs everything locally with one command, creates the SNS/SQS resources in AWS, and shows logs in a dashboard.

Work through the steps in order. Each one ends with a **Check it** so you know it worked before moving on.

---

## Step 1: Install the tools and create the solution

### 1a. Templates and tools

```bash
dotnet new install Aspire.ProjectTemplates      # project templates for the Aspire AppHost
dotnet tool install -g Amazon.Lambda.Tools      # `dotnet lambda` command, used for deploying in Step 6
```

### 1b. Solution

```bash
cd /home/jian/code/personal/dotnetaws
mkdir orders-serverless && cd orders-serverless
dotnet new sln --name OrdersServerless
dotnet new gitignore
```

A solution (`.sln`) groups several projects so one `dotnet build` builds them all. This app has four projects.

### 1c. Shared contract: `Orders.Contracts`

```bash
dotnet new classlib --name Orders.Contracts --output src/Orders.Contracts
dotnet sln add src/Orders.Contracts
rm src/Orders.Contracts/Class1.cs
```

Create `src/Orders.Contracts/OrderCreated.cs`:

```csharp
namespace Orders.Contracts;

public record OrderCreated(Guid OrderId, string CustomerEmail, decimal Total, DateTime CreatedAt);
```

The API sends this message and the processor receives it. Keeping it in one shared library means both sides
always agree on its shape. A `record` is immutable, which suits a message: it shouldn't change after it's sent.

**Check it:** `dotnet build` → `Build succeeded`.

---

## Step 2: `Orders.Api`, a minimal API that runs in Lambda

### 2a. Create the project

```bash
dotnet new web --name Orders.Api --output src/Orders.Api
dotnet sln add src/Orders.Api
dotnet add src/Orders.Api reference src/Orders.Contracts
dotnet add src/Orders.Api package Amazon.Lambda.AspNetCoreServer.Hosting
dotnet add src/Orders.Api package AWSSDK.SimpleNotificationService
dotnet add src/Orders.Api package AWSSDK.Extensions.NETCore.Setup
```

| Package | Why |
|---|---|
| `Amazon.Lambda.AspNetCoreServer.Hosting` | Lets a normal ASP.NET Core app run inside Lambda |
| `AWSSDK.SimpleNotificationService` | The SNS client (`IAmazonSimpleNotificationService`) |
| `AWSSDK.Extensions.NETCore.Setup` | `AddAWSService<T>()`, the same registration you used for S3 in `youtube-demo-webapi` |

### 2b. Write `src/Orders.Api/Program.cs`

Replace the whole file:

```csharp
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
```

Things to notice:
- The API doesn't process the order itself. It publishes an event and returns. That keeps the API fast, and the
  slow work happens in the processor.
- No AWS keys in code. Credentials come from your AWS profile locally and from the Lambda's IAM role in AWS,
  the same idea as the `IsDevelopment()` discussion in `youtube-demo-webapi`.

**Check it:** `dotnet build` → `Build succeeded`.

---

## Step 3: `Orders.Processor`, the Lambda that SQS triggers

### 3a. Create the project

```bash
dotnet new classlib --name Orders.Processor --output src/Orders.Processor
dotnet sln add src/Orders.Processor
dotnet add src/Orders.Processor reference src/Orders.Contracts
dotnet add src/Orders.Processor package Amazon.Lambda.Core
dotnet add src/Orders.Processor package Amazon.Lambda.SQSEvents
dotnet add src/Orders.Processor package Amazon.Lambda.Serialization.SystemTextJson
rm src/Orders.Processor/Class1.cs
```

A Lambda function is just a class library with a handler method. Lambda loads the DLL and calls the method.

Add these two lines inside the existing `<PropertyGroup>` in `src/Orders.Processor/Orders.Processor.csproj`:

```xml
<GenerateRuntimeConfigurationFiles>true</GenerateRuntimeConfigurationFiles>
<AWSProjectType>Lambda</AWSProjectType>
```

The first makes the build output include the runtime config Lambda needs; the second tells the AWS tools it's a Lambda project.

### 3b. Write `src/Orders.Processor/Function.cs`

```csharp
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
```

Things to notice:
- SQS delivers messages in **batches**. Returning the failed IDs in `SQSBatchResponse` means one bad message
  doesn't make the whole batch retry.
- `message.Body` is the order JSON directly because the SNS→SQS subscription uses **raw message delivery**
  (set up in Step 4). Without it, the body would be an SNS envelope with the order JSON inside a `Message` field.

**Check it:** `dotnet build` → `Build succeeded`.

---

## Step 4: `Orders.AppHost`, Aspire runs it all

### 4a. AWS credentials for Aspire

Aspire creates the SNS topic and SQS queue in your real AWS account, so it needs credentials. It reads them from an
AWS CLI profile (not from `secret.json`, which only `youtube-demo-webapi` reads). Use your IAM user's keys, not root:

```bash
aws configure --profile youtube-demo     # access key, secret key, region ca-central-1, output json
aws sts get-caller-identity --profile youtube-demo   # should show your IAM user, not root
```

The IAM user needs permission for CloudFormation, SNS and SQS (for learning, the managed policies
`AWSCloudFormationFullAccess`, `AmazonSNSFullAccess`, `AmazonSQSFullAccess` are enough).

### 4b. Define the AWS resources: `src/Orders.AppHost/orders.template.json`

Create the AppHost project first:

```bash
dotnet new aspire-apphost --name Orders.AppHost --output src/Orders.AppHost
dotnet sln add src/Orders.AppHost
dotnet add src/Orders.AppHost reference src/Orders.Api src/Orders.Processor
dotnet add src/Orders.AppHost package Aspire.Hosting.AWS
```

Then create `src/Orders.AppHost/orders.template.json`, a CloudFormation template:

```json
{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "OrdersTopic": { "Type": "AWS::SNS::Topic" },

    "OrdersDeadLetterQueue": { "Type": "AWS::SQS::Queue" },

    "OrdersQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "RedrivePolicy": {
          "deadLetterTargetArn": { "Fn::GetAtt": ["OrdersDeadLetterQueue", "Arn"] },
          "maxReceiveCount": 3
        }
      }
    },

    "OrdersQueuePolicy": {
      "Type": "AWS::SQS::QueuePolicy",
      "Properties": {
        "Queues": [{ "Ref": "OrdersQueue" }],
        "PolicyDocument": {
          "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "sns.amazonaws.com" },
            "Action": "sqs:SendMessage",
            "Resource": { "Fn::GetAtt": ["OrdersQueue", "Arn"] },
            "Condition": { "ArnEquals": { "aws:SourceArn": { "Ref": "OrdersTopic" } } }
          }]
        }
      }
    },

    "OrdersSubscription": {
      "Type": "AWS::SNS::Subscription",
      "Properties": {
        "TopicArn": { "Ref": "OrdersTopic" },
        "Protocol": "sqs",
        "Endpoint": { "Fn::GetAtt": ["OrdersQueue", "Arn"] },
        "RawMessageDelivery": true
      }
    }
  },
  "Outputs": {
    "OrdersTopicArn": { "Value": { "Ref": "OrdersTopic" } },
    "OrdersQueueUrl": { "Value": { "Ref": "OrdersQueue" } },
    "OrdersQueueArn": { "Value": { "Fn::GetAtt": ["OrdersQueue", "Arn"] } }
  }
}
```

What each resource is for:
- **OrdersTopic**: the SNS topic the API publishes to.
- **OrdersQueue + OrdersDeadLetterQueue**: the queue the processor reads. After 3 failed attempts a message
  moves to the dead-letter queue instead of retrying forever.
- **OrdersQueuePolicy**: SQS refuses messages by default; this allows *only this topic* to send to the queue.
- **OrdersSubscription**: connects the topic to the queue, with raw delivery (see Step 3).
- **Outputs**: values Aspire passes to your projects as configuration, e.g. `AWS:Resources:OrdersTopicArn`.

> The video uses an AWS CDK stack for this. A CloudFormation template does the same job without needing
> Node.js and the CDK CLI, and you can read exactly what gets created. `Aspire.Hosting.AWS` supports both.

### 4c. Write `src/Orders.AppHost/AppHost.cs`

The template may name the file `Program.cs`; either way, replace its contents:

```csharp
using Amazon;
using Aspire.Hosting.AWS.Lambda;

#pragma warning disable CA2252 // Aspire's Lambda support is marked as preview

var builder = DistributedApplication.CreateBuilder(args);

var awsConfig = builder.AddAWSSDKConfig()
    .WithProfile("youtube-demo")
    .WithRegion(RegionEndpoint.CACentral1);

// Creates (or updates) the CloudFormation stack in your AWS account when the AppHost starts
var ordersResources = builder.AddAWSCloudFormationTemplate("orders-resources", "orders.template.json")
    .WithReference(awsConfig);

// Orders.Api runs locally as a Lambda function, behind a local API Gateway emulator
var ordersApi = builder.AddAWSLambdaFunction<Projects.Orders_Api>("OrdersApi", lambdaHandler: "Orders.Api")
    .WithReference(awsConfig)
    .WithReference(ordersResources);   // injects AWS:Resources:OrdersTopicArn etc.

builder.AddAWSAPIGatewayEmulator("APIGatewayEmulator", APIGatewayType.HttpV2)
    .WithReference(ordersApi, Method.Any, "/{proxy+}");

// Orders.Processor runs locally as a Lambda function, triggered by messages in the real SQS queue
builder.AddAWSLambdaFunction<Projects.Orders_Processor>("OrdersProcessor",
        lambdaHandler: "Orders.Processor::Orders.Processor.Function::FunctionHandler")
    .WithReference(awsConfig)
    .WithSQSEventSource(ordersResources, "OrdersQueueUrl");

builder.Build().Run();
```

Things to notice:
- **`lambdaHandler`**: for an ASP.NET Core app (Orders.Api) it's just the assembly name. For a class library
  it's `Assembly::Namespace.Class::Method`.
- **`APIGatewayType.HttpV2`** matches `LambdaEventSource.HttpApi` in Step 2. If they don't match, requests fail.
- **The SNS topic and SQS queue are real** (in AWS); only the Lambda functions and API Gateway are emulated locally.
  Aspire installs the Lambda Test Tool it needs automatically on first run.

> `Aspire.Hosting.AWS`'s Lambda APIs are newer and have changed between versions. If a method above doesn't
> compile, especially `WithSQSEventSource`, check the overloads in your IDE (Ctrl+Space) or the package's
> README on GitHub (`aws/integrations-on-dotnet-aspire-for-aws`) for the version you installed.

**Check it:** `dotnet build` → `Build succeeded`.

---

## Step 5: Run and test locally

```bash
dotnet run --project src/Orders.AppHost
```

1. The console prints a **dashboard URL** (with a login token). Open it.
2. Wait for `orders-resources` to finish. The first run creates the CloudFormation stack, which takes a minute or two.
   You can watch it in the AWS console under CloudFormation (region ca-central-1).
3. Find the **APIGatewayEmulator** URL in the dashboard and send an order:

```bash
curl -i -X POST <APIGatewayEmulator-url>/orders \
  -H "Content-Type: application/json" \
  -d '{"customerEmail":"test@example.com","total":42.50}'
```

**Check it:**
- curl shows `HTTP/1.1 202 Accepted` and an `orderId`.
- In the dashboard, the **OrdersProcessor** logs show `Processing order <that id> for test@example.com, total 42.50`.

The request went: curl → API Gateway emulator → Orders.Api → **SNS (AWS)** → **SQS (AWS)** → Orders.Processor.

---

## Step 6: Deploy to AWS (optional)

So far only SNS and SQS are in AWS. To run the Lambdas in AWS too:

### 6a. Target framework

Lambda's managed runtimes lag behind .NET releases. In the Lambda console (Create function → Runtime), check
whether **.NET 10** is listed. If it isn't, change `<TargetFramework>` to `net8.0` in `Orders.Contracts`,
`Orders.Api` and `Orders.Processor`, and use `--function-runtime dotnet8` below.

### 6b. Get the resource values

```bash
aws cloudformation describe-stacks --stack-name orders-resources --profile youtube-demo \
  --query "Stacks[0].Outputs" --output table
```

(If the stack has a different name, find it in the CloudFormation console.)

### 6c. Deploy the processor and connect it to the queue

```bash
cd src/Orders.Processor
dotnet lambda deploy-function OrdersProcessor \
  --function-handler "Orders.Processor::Orders.Processor.Function::FunctionHandler" \
  --function-runtime dotnet10 --region ca-central-1 --profile youtube-demo
# when asked for a role: create a new one with the AWSLambdaSQSQueueExecutionRole policy

aws lambda create-event-source-mapping --function-name OrdersProcessor \
  --event-source-arn <OrdersQueueArn> --function-response-types ReportBatchItemFailures \
  --profile youtube-demo
```

`ReportBatchItemFailures` makes Lambda respect the `SQSBatchResponse` from Step 3.

### 6d. Deploy the API and put API Gateway in front

```bash
cd ../Orders.Api
dotnet lambda deploy-function OrdersApi \
  --function-handler Orders.Api --function-runtime dotnet10 \
  --environment-variables "AWS__Resources__OrdersTopicArn=<OrdersTopicArn>" \
  --region ca-central-1 --profile youtube-demo
# role: create a new one with AWSLambdaBasicExecutionRole, then add sns:Publish on the topic to it

aws apigatewayv2 create-api --name orders-api --protocol-type HTTP \
  --target <OrdersApi-function-arn> --profile youtube-demo
aws lambda add-permission --function-name OrdersApi --statement-id apigw \
  --action lambda:InvokeFunction --principal apigateway.amazonaws.com --profile youtube-demo
```

`AWS__Resources__OrdersTopicArn` becomes the config key `AWS:Resources:OrdersTopicArn` (`__` = `:`), so
`Program.cs` doesn't change between local and AWS.

**Check it:** `curl -X POST <ApiEndpoint from create-api>/orders ...` returns 202, and the order appears in
CloudWatch Logs under `/aws/lambda/OrdersProcessor`.

---

## Clean up

Delete what you created so nothing keeps costing money:

```bash
aws cloudformation delete-stack --stack-name orders-resources --profile youtube-demo   # topic, queues, subscription
# if you did Step 6:
aws lambda delete-function --function-name OrdersApi --profile youtube-demo
aws lambda delete-function --function-name OrdersProcessor --profile youtube-demo
aws apigatewayv2 delete-api --api-id <api-id> --profile youtube-demo
```

Also delete the IAM roles `dotnet lambda` created (IAM console → Roles).
