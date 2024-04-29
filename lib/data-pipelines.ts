import * as path from 'path'
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as scheduler from "aws-cdk-lib/aws-scheduler";
import * as python from "@aws-cdk/aws-lambda-python-alpha";
import { Construct } from 'constructs';
import { DockerImage, Duration } from 'aws-cdk-lib';
import Pipeline, { lambdaRds } from './data-pipeline';

export interface DataPipelinesProps {
  s3RawData: s3.IBucket;
  rds: lambdaRds;
  transformLoadRole: iam.IRole;
}

export default class DataPipelines extends Construct {
  constructor(scope: Construct, id: string, props: DataPipelinesProps) {
    super(scope, id)
    const httpCallLambda = new lambda.Function(this, 'HttpCallLambdaFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'http_call.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, 'files/http-call')),
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.seconds(10),
    });
    props.s3RawData.grantWrite(httpCallLambda);

    const httpCallLambdaVersion = httpCallLambda.currentVersion;
    const httpCallLambdaAlias = new lambda.Alias(this, 'HttpCallLambdaAlias', {
      aliasName: 'Current',
      version: httpCallLambdaVersion,
      provisionedConcurrentExecutions: 0,
    });

    const cfnScheduleGroup = new scheduler.CfnScheduleGroup(this, 'DataPipelinesScheduleGroup', {
      name: 'data-pipelines',
    });

    const sqlalchemyLayer = new python.PythonLayerVersion(this, 'SqlalchemyLayerVersion', {
      entry: path.join(__dirname, 'files/layer-sqlalchemy'),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_11],
      bundling: {
        image: DockerImage.fromBuild(path.join(__dirname, 'files/layer-sqlalchemy')),
        platform: 'linux/arm64'
      }
    })

    new Pipeline(this, 'DitTradeBarriers', {
      datasetProvider: 'dit',
      datasetName: 'trade-barriers',
      datasetType: 'json',
      scheduleExpression: "cron(0 */4 * * ? *)",
      s3RawData: props.s3RawData,
      extractLambda: httpCallLambdaAlias,
      scheduleGroupName: cfnScheduleGroup.name!,
      extractConfig: {
        url: 'https://data.api.trade.gov.uk/v1/datasets/market-barriers/versions/v1.0.10/data?format=json',
      },
      transformLoadConfig: {
        path: path.join(__dirname, 'files/dit-trade-barriers-tl'),
        handler: 'main.handler',
        memorySize: 512,
        timeout: Duration.seconds(20),
        layerArns: [
          'arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311-Arm64:4',
          sqlalchemyLayer.layerVersionArn,
        ],
        rds: props.rds,
        role: props.transformLoadRole,
      },
    });

    new Pipeline(this, 'EnvironmentAgencyFloods', {
      datasetProvider: 'Environment-Agency',
      datasetName: 'Floods',
      datasetType: 'json',
      scheduleExpression: "cron(*/30 * * * ? *)",
      s3RawData: props.s3RawData,
      extractLambda: httpCallLambdaAlias,
      scheduleGroupName: cfnScheduleGroup.name!,
      extractConfig: {
        url: 'https://environment.data.gov.uk/flood-monitoring/id/floods',
      },
      transformLoadConfig: {
        path: path.join(__dirname, 'files/ea-floods-tl'),
        handler: 'main.handler',
        layerArns: [
          'arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311-Arm64:4',
          sqlalchemyLayer.layerVersionArn,
        ],
        rds: props.rds,
        role: props.transformLoadRole,
      },
    });

    new Pipeline(this, 'EnvironmentAgencyFloodAreas', {
      datasetProvider: 'Environment-Agency',
      datasetName: 'Flood-Areas',
      datasetType: 'json',
      scheduleExpression: "cron(*/30 * * * ? *)",
      s3RawData: props.s3RawData,
      extractLambda: httpCallLambdaAlias,
      scheduleGroupName: cfnScheduleGroup.name!,
      extractConfig: {
        url: 'https://environment.data.gov.uk/flood-monitoring/id/floodAreas?_limit=99999',
      },
      transformLoadConfig: {
        path: path.join(__dirname, 'files/ea-flood-areas-tl'),
        docker: true,
        handler: 'main.handler',
        architecture: lambda.Architecture.X86_64,
        rds: props.rds,
        role: props.transformLoadRole,
      },
    });

    new Pipeline(this, 'NhsUecSitrep', {
      datasetProvider: 'NHS',
      datasetName: 'UEC-Sitrep',
      datasetType: 'xlsx',
      scheduleExpression: "cron(45 9 ? * FRI *)",
      s3RawData: props.s3RawData,
      extractLambda: httpCallLambdaAlias,
      scheduleGroupName: cfnScheduleGroup.name!,
      extractConfig: {
        url: 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2023/12/Web-File-Timeseries-UEC-Daily-SitRep-2.xlsx',
      },
      transformLoadConfig: {
        path: path.join(__dirname, 'files/nhs-uec-sitrep-tl'),
        handler: 'main.handler',
        layerArns: [
          'arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311-Arm64:8',
          sqlalchemyLayer.layerVersionArn
        ],
        rds: props.rds,
        role: props.transformLoadRole,
      },
    });
  }
}
