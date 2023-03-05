#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { CdkGlueDynamodbStack } from "../lib/cdk-glue-dynamodb-stack";

const app = new cdk.App();

// Create some tables with the same prefix
new CdkGlueDynamodbStack(app, "GlueDynamodbStackJavav2Netty", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  tableName: "ecs-javav2-netty-60",
});

new CdkGlueDynamodbStack(app, "GlueDynamodbStackJavav2CRT", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  tableName: "ecs-javav2-crt-60",
});

new CdkGlueDynamodbStack(app, "GlueDynamodbStackBoto3", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  tableName: "ecs-boto3-60",
});

new CdkGlueDynamodbStack(app, "GlueDynamodbStackJSv3", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  tableName: "ecs-jsv3-60",
});
