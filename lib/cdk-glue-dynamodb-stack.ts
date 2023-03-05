import {
  Duration,
  Stack,
  StackProps,
  aws_s3_assets,
  aws_dynamodb,
  RemovalPolicy,
  CfnOutput,
} from "aws-cdk-lib";
import * as glue from "@aws-cdk/aws-glue-alpha";
import { Construct } from "constructs";

interface CdkGlueDynamodbStackProps extends StackProps {
  tableName: string;
}

export class CdkGlueDynamodbStack extends Stack {
  constructor(scope: Construct, id: string, props: CdkGlueDynamodbStackProps) {
    super(scope, id, props);

    const assetName = (name: String) => `${this.stackName}-${name}`;
    const tableName = props.tableName;
    const workerCount = 3;

    const table =
      // aws_dynamodb.Table.fromTableName(this, tableName, tableName) ??
      new aws_dynamodb.Table(this, assetName(tableName), {
        partitionKey: { name: "pk", type: aws_dynamodb.AttributeType.STRING },
        billingMode: aws_dynamodb.BillingMode.PAY_PER_REQUEST,
        tableName,
        removalPolicy: RemovalPolicy.RETAIN,
      });

    // Can't import directly due to this bug below, have to go via S3.
    // https://github.com/aws/aws-cdk/issues/20481

    const glue_s3_asset = new aws_s3_assets.Asset(
      this,
      assetName("s3-script-asset"),
      {
        path: "./lib/job/create-data.py",
      }
    );

    // Glue ETL Job
    const glueJob = new glue.Job(this, assetName("CreateDataJob"), {
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
      workerCount: workerCount,
      maxRetries: 0,
      timeout: Duration.minutes(60),
      jobName: `SeedDataJob-${tableName}`,
      defaultArguments: {
        "--pk_index": "0",
        "--table_name": tableName,
        "--item_size_kb": "2",
        "--number_of_items": "100000",
        "--partition_count": workerCount.toString(),
      },
    });

    // Grant permissions to the Glue Job to access the DynamoDB table
    table.grantWriteData(glueJob.role);

    // Output job name
    new CfnOutput(this, assetName("RunJobDesc"), {
      value: `aws glue start-job-run --job-name ${glueJob.jobName}`,
    });
  }
}
