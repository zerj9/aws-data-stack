import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as scheduler from "aws-cdk-lib/aws-scheduler";
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';
import { Duration } from 'aws-cdk-lib';

export interface extractConfig {
  url: string;
}

export interface lambdaNetwork {
  vpc: ec2.IVpc;
  vpcSubnets: ec2.SubnetSelection;
  securityGroups?: ec2.ISecurityGroup[];
  allowPublicSubnet: boolean;
}

export interface lambdaRds {
  network: lambdaNetwork
  secret: secretsmanager.ISecret;
}

export interface transformLoadConfig {
  path: string;
  handler: string;
  role: iam.IRole;
  docker?: boolean;
  memorySize?: number;
  architecture?: lambda.Architecture;
  layerArns?: string[];
  rds?: lambdaRds;
}

export interface PipelineProps {
  datasetProvider: string;
  datasetName: string;
  datasetType: string;
  scheduleExpression: string;
  s3RawData: s3.IBucket;
  extractLambda: lambda.IFunction;
  scheduleGroupName: string;
  extractConfig: extractConfig;
  transformLoadConfig: transformLoadConfig;
}

export default class Pipeline extends Construct {
  constructor(scope: Construct, id: string, props: PipelineProps) {
    super(scope, id)

    const getDataLambda = new tasks.LambdaInvoke(this, 'InvokeHttpCallLambda', {
      lambdaFunction: props.extractLambda,
    });

    let transformLoadLambda;
    if (!props.transformLoadConfig.docker) {
      transformLoadLambda = new lambda.Function(this, 'TransformLoadLambdaFunction', {
        runtime: lambda.Runtime.PYTHON_3_11,
        handler: props.transformLoadConfig.handler,
        code: lambda.Code.fromAsset(props.transformLoadConfig.path),
        memorySize: props.transformLoadConfig.memorySize || 256,
        architecture: props.transformLoadConfig.architecture || lambda.Architecture.ARM_64,
        role: props.transformLoadConfig.role,
        timeout: Duration.seconds(10),
        ...(props.transformLoadConfig.rds?.network ? props.transformLoadConfig.rds.network : {}),
        environment: {
          ...(props.transformLoadConfig.rds ? { SECRET_NAME: props.transformLoadConfig.rds.secret.secretName } : {}),
        },
      });
    } else {
      transformLoadLambda = new lambda.DockerImageFunction(this, 'TransformLoadLambdaFunction', {
        code: lambda.DockerImageCode.fromImageAsset(props.transformLoadConfig.path),
        memorySize: props.transformLoadConfig.memorySize || 256,
        architecture: props.transformLoadConfig.architecture || lambda.Architecture.ARM_64,
        role: props.transformLoadConfig.role,
        timeout: Duration.seconds(10),
        ...(props.transformLoadConfig.rds?.network ? props.transformLoadConfig.rds.network : {}),
        environment: {
          ...(props.transformLoadConfig.rds ? { SECRET_NAME: props.transformLoadConfig.rds.secret.secretName } : {}),
        },
      });
    }

    if (props.transformLoadConfig.layerArns) {
      const layerVersions: lambda.ILayerVersion[] = props.transformLoadConfig.layerArns.map((arn) => (
        lambda.LayerVersion.fromLayerVersionArn(
          this,
          arn.split(":").slice(-2).join(":"),
          arn
        ))
      )
      transformLoadLambda.addLayers(...layerVersions)
    }

    const invokeTransformLoadLambda = new tasks.LambdaInvoke(this, 'InvokeTransformLoadLambda', {
      lambdaFunction: transformLoadLambda,
    });

    const passConfig = new sfn.Pass(this, 'ConfigData', {
      result: sfn.Result.fromObject({
        datasetProvider: props.datasetProvider,
        datasetName: props.datasetName,
        datasetType: props.datasetType,
        rawBucket: props.s3RawData.bucketName,
        ...props.extractConfig
      }),
      resultPath: '$.config',
    });

    const definition = passConfig.next(getDataLambda).next(invokeTransformLoadLambda);

    const stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      timeout: Duration.minutes(1),
    });

    const schedulerRole = new iam.Role(this, 'SchedulerRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    stateMachine.grantStartExecution(schedulerRole);
    stateMachine.grantStartSyncExecution(schedulerRole);

    new scheduler.CfnSchedule(this, 'Schedule', {
      scheduleExpression: props.scheduleExpression,
      flexibleTimeWindow: {
        mode: "OFF",
      },
      target: {
        arn: stateMachine.stateMachineArn,
        roleArn: schedulerRole.roleArn,
      },
      description: `${props.datasetProvider} - ${props.datasetName}`,
      name: `${props.datasetProvider}-${props.datasetName}`,
      groupName: props.scheduleGroupName,
      state: "ENABLED",
    });
  }
}
