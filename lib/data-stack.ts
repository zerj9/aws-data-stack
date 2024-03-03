import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import DataPipelines from './data-pipelines';
import DataWarehouse from './data-warehouse';

export interface DataStackProps extends cdk.StackProps {
  network: {
    vpc: ec2.IVpc;
    subnetGroup: rds.SubnetGroup;
  }
}

export class DataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DataStackProps) {
    super(scope, id, props);

    const rawDataBucket = new s3.Bucket(this, 'RawDataBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const pipelineTransformLoadRole = new iam.Role(this, 'PipelineTransformLoadRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    pipelineTransformLoadRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole")
    );
    pipelineTransformLoadRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaENIManagementAccess")
    );
    rawDataBucket.grantRead(pipelineTransformLoadRole);

    const warehouse = new DataWarehouse(this, 'Warehouse', {
      network: {
        vpc: props.network.vpc,
        subnetGroup: props.network.subnetGroup,
      }
    })

    const transformLambdaSecurityGroup = new ec2.SecurityGroup(this, 'TransformLambdaSecurityGroup', {
      vpc: props.network.vpc,
      description: 'Used by data pipeline lambda transform functions',
      allowAllOutbound: true,
      disableInlineRules: true
    });
    warehouse.dataWarehouseSecurityGroup.addIngressRule(
      transformLambdaSecurityGroup,
      ec2.Port.tcp(5432),
      'transform lambda to data warehouse'
    )

    new DataPipelines(this, 'Pipelines', {
      s3RawData: rawDataBucket,
      transformLoadRole: pipelineTransformLoadRole,
      rds: {
        network: {
          vpc: props.network.vpc,
          vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
          securityGroups: [transformLambdaSecurityGroup],
          allowPublicSubnet: true,
        },
        secret: warehouse.warehouseWriteSecret
      } 
    });
  }
}
