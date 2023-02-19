import {
  Duration,
  Stack,
  StackProps,
  aws_s3_assets,
  aws_dynamodb,
  RemovalPolicy,
} from "aws-cdk-lib";
import * as glue from "@aws-cdk/aws-glue-alpha";
import { Construct } from "constructs";

export class CdkGlueDynamodbStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const tableName = "load-test-tbl1";

    // DynamoDB Table
    const table = new aws_dynamodb.Table(this, "LoadTestTbl", {
      partitionKey: { name: "pk", type: aws_dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: aws_dynamodb.AttributeType.STRING },
      billingMode: aws_dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // Can't import python directly due to this bug below, have to go via S3.
    // https://github.com/aws/aws-cdk/issues/20481

    const glue_s3_asset = new aws_s3_assets.Asset(this, "asset", {
      path: "./lib/job/create-data.py",
    });

    // Glue ETL Job
    const glueJob = new glue.Job(this, "CreateDataJob", {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V4_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromBucket(
          glue_s3_asset.bucket,
          glue_s3_asset.s3ObjectKey
        ),
      }),
      description: "Python ETL job to copy table to another account",
      workerType: glue.WorkerType.G_1X,
      workerCount: 3,
      maxRetries: 0,
      timeout: Duration.minutes(60),
      jobName: "CreateDataJob",
      defaultArguments: {
        "--table_name": table.tableName,
        "--item_size_kb": "2",
        "--number_of_items": "100000",
      },
    });

    table.grantFullAccess(glueJob.grantPrincipal);
  }
}
