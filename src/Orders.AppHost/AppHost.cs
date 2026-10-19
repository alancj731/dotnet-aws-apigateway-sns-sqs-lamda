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
var ordersProcessor = builder.AddAWSLambdaFunction<Projects.Orders_Processor>("OrdersProcessor",
        lambdaHandler: "Orders.Processor::Orders.Processor.Function::FunctionHandler")
    .WithReference(awsConfig);

// WithSQSEventSource needs the real queue URL at startup, before the stack above is created,
// so it comes from config (user secret, set from the stack's OrdersQueueUrl output after the first run)
var ordersQueueUrl = builder.Configuration["OrdersQueueUrl"];
if (!string.IsNullOrEmpty(ordersQueueUrl))
    ordersProcessor.WithSQSEventSource(ordersQueueUrl);

builder.Build().Run();