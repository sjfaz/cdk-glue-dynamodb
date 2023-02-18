# CDK project to create a DynamoDB table and a Glue script

### This project is for generating data for a load test.

```
# Deploy
cdk deploy
# To kick off the job
aws glue start-job-run --job-name CreateDataJob
```

## Useful commands

- `npm run build` compile typescript to js
- `npm run watch` watch for changes and compile
- `npm run test` perform the jest unit tests
- `cdk deploy` deploy this stack to your default AWS account/region
- `cdk diff` compare deployed stack with current state
- `cdk synth` emits the synthesized CloudFormation template
