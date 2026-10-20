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
var ordersProcessor = builder.AddAWSLambdaFunction<Projects.Orders_Processor>("OrdersProcessor",
        lambdaHandler: "Orders.Processor::Orders.Processor.Function::FunctionHandler")
    .WithReference(awsConfig);

// WithSQSEventSource needs the real queue URL at startup, before the stack above is created,
// so it comes from config (user secret, set from the stack's OrdersQueueUrl output after the first run)
var ordersQueueUrl = builder.Configuration["OrdersQueueUrl"];
if (!string.IsNullOrEmpty(ordersQueueUrl))
    ordersProcessor.WithSQSEventSource(ordersQueueUrl);

builder.Build().Run();
```

Things to notice:
- **`lambdaHandler`**: for an ASP.NET Core app (Orders.Api) it's just the assembly name. For a class library
  it's `Assembly::Namespace.Class::Method`.
- **`APIGatewayType.HttpV2`** matches `LambdaEventSource.HttpApi` in Step 2. If they don't match, requests fail.
- **The SNS topic and SQS queue are real** (in AWS); only the Lambda functions and API Gateway are emulated locally.
  Aspire installs the Lambda Test Tool it needs automatically on first run.

- **The queue URL comes from config, not the stack.** In `Aspire.Hosting.AWS` 13.7.2, `WithSQSEventSource` only
  takes the queue URL as a string, and it needs it at startup, before the stack exists. So the first run creates the
  stack without the SQS trigger. After that, save the URL as a user secret and restart:

  ```bash
  url=$(aws cloudformation describe-stacks --stack-name orders-resources --profile youtube-demo \
    --query "Stacks[0].Outputs[?OutputKey=='OrdersQueueUrl'].OutputValue" --output text)
  dotnet user-secrets set OrdersQueueUrl "$url" --project src/Orders.AppHost
  ```

> `Aspire.Hosting.AWS`'s Lambda APIs are newer and have changed between versions. If a method above doesn't
> compile, check the overloads in your IDE (Ctrl+Space) or the package's
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

All commands below pass `--region ca-central-1` explicitly, so they work even if your profile's default region
is different. Run them in one terminal session: later commands use shell variables set by earlier ones.

```bash
aws cloudformation describe-stacks --stack-name orders-resources --profile youtube-demo --region ca-central-1 \
  --query "Stacks[0].Outputs" --output table

output() {
  aws cloudformation describe-stacks --stack-name orders-resources --profile youtube-demo --region ca-central-1 \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
topicArn=$(output OrdersTopicArn)
queueArn=$(output OrdersQueueArn)
account=$(aws sts get-caller-identity --profile youtube-demo --query Account --output text)
```

(If the stack has a different name, find it in the CloudFormation console.)

> **Stop the AppHost first.** While it runs, the local `OrdersProcessor` polls the same `OrdersQueue`. With both
> running, each order goes to whichever one picks it up first, so it can look like the AWS processor missed it.

### 6c. Deploy the processor and connect it to the queue

```bash
cd src/Orders.Processor
dotnet lambda deploy-function OrdersProcessor \
  --function-handler "Orders.Processor::Orders.Processor.Function::FunctionHandler" \
  --function-runtime dotnet10 --region ca-central-1 --profile youtube-demo
```

When it asks for a role, choose **Create new IAM Role**, name it `orders-processor-role`, and pick
**`AWSLambdaSQSQueueExecutionRole`** (receive/delete SQS messages, plus CloudWatch Logs). Don't pick a
`AWSLambdaBasicExecutionRole-<guid>` entry: those are copies the Lambda console made for another function, and
they only allow writing to *that* function's log group.

```bash
aws lambda create-event-source-mapping --function-name OrdersProcessor \
  --event-source-arn $queueArn --function-response-types ReportBatchItemFailures \
  --profile youtube-demo --region ca-central-1
```

`ReportBatchItemFailures` makes Lambda respect the `SQSBatchResponse` from Step 3.

### 6d. Deploy the API

```bash
cd ../Orders.Api
dotnet lambda deploy-function OrdersApi \
  --function-handler Orders.Api --function-runtime dotnet10 \
  --environment-variables "AWS__Resources__OrdersTopicArn=$topicArn" \
  --region ca-central-1 --profile youtube-demo
```

When it asks for a role, choose **Create new IAM Role** again (don't reuse the processor's), name it
`orders-api-role`, and pick **`AWSLambdaBasicExecutionRole`** (option without a `-<guid>` suffix). That only
covers logs, so allow publishing to the topic too, or every request fails with a 500
(`not authorized to perform: SNS:Publish`):

```bash
aws iam put-role-policy --role-name orders-api-role --policy-name publish-orders-topic --profile youtube-demo \
  --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"sns:Publish\",\"Resource\":\"$topicArn\"}]}"
```

`AWS__Resources__OrdersTopicArn` becomes the config key `AWS:Resources:OrdersTopicArn` (`__` = `:`), so
`Program.cs` doesn't change between local and AWS.

### 6e. Put API Gateway in front

```bash
fnArn=$(aws lambda get-function --function-name OrdersApi --profile youtube-demo --region ca-central-1 \
  --query "Configuration.FunctionArn" --output text)   # arn:aws:lambda:...:function:OrdersApi

aws apigatewayv2 create-api --name orders-api --protocol-type HTTP --target $fnArn \
  --profile youtube-demo --region ca-central-1

apiId=$(aws apigatewayv2 get-apis --profile youtube-demo --region ca-central-1 \
  --query "Items[?Name=='orders-api'].ApiId" --output text)
endpoint=$(aws apigatewayv2 get-apis --profile youtube-demo --region ca-central-1 \
  --query "Items[?Name=='orders-api'].ApiEndpoint" --output text)

aws lambda add-permission --function-name OrdersApi --statement-id apigw \
  --action lambda:InvokeFunction --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:ca-central-1:$account:$apiId/*" \
  --profile youtube-demo --region ca-central-1
```

- `--target` turns on "quick create": one command makes the HTTP API, a Lambda proxy integration, a catch-all
  `$default` route and an auto-deployed `$default` stage. ASP.NET Core does the routing, so `POST /orders` works.
- `create-api` doesn't let API Gateway call the function; `add-permission` does. `--source-arn` limits that
  to this one API (without it, any API Gateway could invoke `OrdersApi`).
- Use the **Function ARN** (`...:function:OrdersApi`), not the *Runtime version ARN* (`...::runtime:<hash>`)
  shown under Runtime settings in the console.

**Check it:**

```bash
curl -i -X POST $endpoint/orders \
  -H "Content-Type: application/json" \
  -d '{"customerEmail":"test@example.com","total":42.50}'

aws logs tail /aws/lambda/OrdersProcessor --since 5m --follow --profile youtube-demo --region ca-central-1
```

- curl shows `HTTP/2 202` and an `orderId`.
- The log shows `Processing order <that id> for test@example.com, total 42.50` (Ctrl+C to stop following).

If it doesn't work:

- **500 from curl**: read the API's own log, `aws logs tail /aws/lambda/OrdersApi --since 5m ...`. Usual causes
  are a missing `sns:Publish` on `orders-api-role`, a wrong `OrdersTopicArn`, or skipping `add-permission`.
- **202 but nothing in the processor log**: check the AppHost isn't still running (see 6b), the event source
  mapping exists (`aws lambda list-event-source-mappings --function-name OrdersProcessor ...`), and the
  processor's role has `AWSLambdaSQSQueueExecutionRole`.

---

## Step 7: CI/CD, deploy the stack from GitHub Actions (optional)

Locally, the AppHost sends `orders.template.json` to CloudFormation every time it starts. In a real deployment
the AppHost doesn't run, so something else has to do it. In a real project that's a pipeline that deploys on
every merge to `main`, so nobody deploys from their laptop.

This step uses a separate stack, **`orders-prod`**, so the pipeline never touches the `orders-resources` stack
your local AppHost owns. (If you do Step 6 against the pipeline's stack, read the outputs from `orders-prod` in 6b.)

### 7a. Let GitHub sign in to AWS without keys (OIDC)

Instead of storing access keys in GitHub, AWS trusts GitHub's identity tokens (OIDC). Each pipeline run gets
temporary credentials for one role, and there's no secret to leak or rotate.

Create the identity provider (once per AWS account):

```bash
aws iam create-open-id-connect-provider --profile youtube-demo \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com
```

A role has two policies: the *trust policy* says **who** may use it (here: GitHub Actions runs from your
repository), and the *permissions policies* say **what** it can do. Write the trust policy to a temporary file
outside the repo (it's only input for `create-role`, so there's nothing to commit).

Run this block as-is in a shell, from the repo folder, **all in one terminal session**. The shell fills in
`${account}`, `${repo}` and `${branch}`. If you write the file by hand or in an editor, they stay literal
(`${account}`); if you run the lines in separate shells, they come out empty (`arn:aws:iam:::...`, `repo::`).
Either way GitHub can't sign in.

```bash
export AWS_PAGER=""   # stop the AWS CLI opening a pager, which pauses the commands that follow

account=$(aws sts get-caller-identity --profile youtube-demo --query Account --output text)
cd "$(git rev-parse --show-toplevel)"   # fails here, not silently below, if you're outside the repo
repo=$(git remote get-url origin | sed -E 's#.*github\.com[:/]##; s#\.git$##')   # e.g. alancj731/my-repo
branch=main   # must match the branch in the workflow's `on: push: branches:` (7b)

# The start of the token's `sub` claim. Newer repos use an "immutable" form with IDs
# (repo:alancj731@114266507/my-repo@1426445702), older ones plain repo:alancj731/my-repo; ask GitHub which.
sub_prefix=$(gh api "repos/${repo}/actions/oidc/customization/sub" --jq .sub_claim_prefix)
echo "$sub_prefix"   # must not be empty; if it is, run `gh auth login` and try again

cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::${account}:oidc-provider/token.actions.githubusercontent.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
        "token.actions.githubusercontent.com:sub": "${sub_prefix}:ref:refs/heads/${branch}"
      }
    }
  }]
}
EOF

grep -E 'Federated|:sub' /tmp/trust-policy.json   # check: a 12-digit account ID and your repo, no ${...} left
```

The `sub` condition means only that branch of *your* repository can use the role. Without it, any GitHub
repository could assume it. It has to match the token exactly: building it by hand as `repo:<user>/<repo>:...`
fails on repositories that use the immutable form, which is why the prefix comes from GitHub. To allow every branch, change `StringEquals` to `StringLike` for the `sub` line
(keep `aud` under `StringEquals`) and use `ref:refs/heads/*`.

Create the role and give it the same permissions as your IAM user in Step 4a (fine for learning; a real
project would narrow these down):

```bash
aws iam create-role --role-name github-actions-orders-deploy \
  --assume-role-policy-document file:///tmp/trust-policy.json --profile youtube-demo
# "Role with name ... already exists"? Update its trust policy instead of deleting it:
#   aws iam update-assume-role-policy --role-name github-actions-orders-deploy \
#     --policy-document file:///tmp/trust-policy.json --profile youtube-demo

for policy in AWSCloudFormationFullAccess AmazonSNSFullAccess AmazonSQSFullAccess; do
  aws iam attach-role-policy --role-name github-actions-orders-deploy \
    --policy-arn arn:aws:iam::aws:policy/$policy --profile youtube-demo
done
```

**Check it:**

```bash
aws iam get-role --role-name github-actions-orders-deploy --profile youtube-demo \
  --query "Role.AssumeRolePolicyDocument.Statement[0].Principal.Federated" --output text
aws iam list-attached-role-policies --role-name github-actions-orders-deploy --profile youtube-demo \
  --query "AttachedPolicies[].PolicyName" --output text
```

The first should show your account ID (not `${account}`), the second the three policies. If the trust policy is
wrong, the workflow fails with `Not authorized to perform sts:AssumeRoleWithWebIdentity` (usually a `sub` that
doesn't match: compare it with `echo "$sub_prefix"` and the branch in the workflow); if policies are missing,
`cloudformation deploy` fails with `AccessDenied`.

Get the role ARN (`arn:aws:iam::<account>:role/github-actions-orders-deploy`) and note it for 7c:

```bash
aws iam get-role --role-name github-actions-orders-deploy --profile youtube-demo --query Role.Arn --output text
```

### 7b. Write `.github/workflows/deploy-infra.yml`

```yaml
name: Deploy infrastructure

on:
  push:
    branches: [main]
    paths: [src/Orders.AppHost/orders.template.json]
  workflow_dispatch:

permissions:
  id-token: write   # lets the job request a GitHub OIDC token
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}
          aws-region: ca-central-1

      - name: Validate template
        run: aws cloudformation validate-template --template-body file://src/Orders.AppHost/orders.template.json

      - name: Deploy stack
        run: |
          aws cloudformation deploy \
            --stack-name orders-prod \
            --template-file src/Orders.AppHost/orders.template.json \
            --no-fail-on-empty-changeset
```

Things to notice:
- **`paths`**: the workflow only runs when the template changes. Code changes don't touch the infrastructure.
  `workflow_dispatch` adds a "Run workflow" button in GitHub so you can also start it by hand.
- **`id-token: write`**: without it, the job can't get the OIDC token and `configure-aws-credentials` fails.
- **`validate-template`** catches JSON and CloudFormation syntax errors before anything is changed.
- **`aws cloudformation deploy`** creates the stack the first time and updates it after that, changing only
  what's different. `--no-fail-on-empty-changeset` makes a run with nothing to change succeed instead of failing.
- The role ARN is a repository **variable**, not a secret: it isn't sensitive, and without the trust policy
  in 7a nobody else can use it.

### 7c. Push and run

```bash
# create an empty repository on GitHub first, then:
git remote add origin git@github.com:<github-user>/<repo>.git
git add .github/workflows/deploy-infra.yml
git commit -m "Deploy infrastructure from GitHub Actions"
git push -u origin main
```

In GitHub: **Settings → Secrets and variables → Actions → Variables → New repository variable**, name
`AWS_DEPLOY_ROLE_ARN`, value the role ARN from 7a. Then **Actions → Deploy infrastructure → Run workflow**.

That one variable is the only thing GitHub needs. Don't add `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` or any
other secret or token: each run gets its own short-lived credentials from the OIDC sign-in set up in 7a.

**Check it:**
- The run in the **Actions** tab is green.
- In the AWS console, CloudFormation (ca-central-1) shows a stack `orders-prod` with the same five resources
  as `orders-resources`.
- Change something small in the template (e.g. add `"Properties": { "DisplayName": "Orders" }` to `OrdersTopic`),
  push to `main`, and the workflow runs again and updates the stack.

> Next step in a real project: move what you did by hand in Step 6 (both Lambdas, their roles, the SQS trigger
> and API Gateway) into the template, or into AWS SAM or CDK, so the pipeline deploys the whole system.

---

## Clean up

Delete what you created so nothing keeps costing money:

```bash
aws cloudformation delete-stack --stack-name orders-resources --profile youtube-demo --region ca-central-1   # topic, queues, subscription
# if you did Step 6:
aws lambda delete-function --function-name OrdersApi --profile youtube-demo --region ca-central-1
aws lambda delete-function --function-name OrdersProcessor --profile youtube-demo --region ca-central-1
apiId=$(aws apigatewayv2 get-apis --profile youtube-demo --region ca-central-1 \
  --query "Items[?Name=='orders-api'].ApiId" --output text)
aws apigatewayv2 delete-api --api-id $apiId --profile youtube-demo --region ca-central-1
# if you did Step 7:
aws cloudformation delete-stack --stack-name orders-prod --profile youtube-demo --region ca-central-1
```

Also delete the IAM roles `dotnet lambda` created, `orders-api-role` and `orders-processor-role` (IAM console →
Roles). If you did Step 7, delete the role
`github-actions-orders-deploy` too, and the GitHub identity provider (IAM console → Identity providers) if
nothing else uses it.
