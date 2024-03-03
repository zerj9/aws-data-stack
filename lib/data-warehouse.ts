import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

export interface DataWarehouseProps {
  network: {
    vpc: ec2.IVpc;
    subnetGroup: rds.SubnetGroup;
  }
}

export default class DataWarehouse extends Construct {
  public readonly warehouseWriteSecret: secretsmanager.ISecret;
  public readonly dataWarehouseSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: DataWarehouseProps) {
    super(scope, id)

    this.dataWarehouseSecurityGroup = new ec2.SecurityGroup(this, 'DataWarehouseSecurityGroup', {
      vpc: props.network.vpc,
      description: 'Used by Data Warehouse',
      allowAllOutbound: true,
      disableInlineRules: true
    });

    const dataWarehouse = new rds.DatabaseInstance(this, "DataWarehouse", {
      engine: rds.DatabaseInstanceEngine.POSTGRES,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.MICRO),
      allocatedStorage: 20,
      credentials: rds.Credentials.fromGeneratedSecret('postgres'),
      vpc: props.network.vpc,
      securityGroups: [this.dataWarehouseSecurityGroup],
      subnetGroup: props.network.subnetGroup,
      storageEncrypted: true,
      caCertificate: rds.CaCertificate.RDS_CA_RDS2048_G1
    });

    const warehouseReadSecret = new secretsmanager.Secret(this, "WarehouseRead", {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          username: "warehouse_read",
          database: "warehouse",
          host: dataWarehouse.dbInstanceEndpointAddress,
          port: dataWarehouse.dbInstanceEndpointPort,
        }),
        generateStringKey: 'password',
        passwordLength: 30,
        excludeCharacters: '"@/\\\'',
      },
      description: "Read only access to the data warehouse"
    });

    this.warehouseWriteSecret = new secretsmanager.Secret(this, "WarehouseWrite", {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          username: "warehouse_write",
          database: "warehouse",
          host: dataWarehouse.dbInstanceEndpointAddress,
          port: dataWarehouse.dbInstanceEndpointPort,
        }),
        generateStringKey: 'password',
        passwordLength: 30,
        excludeCharacters: '"@/\\\'',
      },
      description: "Write access to the data warehouse"
    });

    const dataWarehouseInitSecurityGroup = new ec2.SecurityGroup(this, 'DataWarehouseInitSecurityGroup', {
      vpc: props.network.vpc,
      description: 'Used by Data Warehouse Init Lambda Function',
      allowAllOutbound: true,
      disableInlineRules: true
    });

    // Function to initialize database
    const dataWarehouseInit = new nodejs.NodejsFunction(this, 'DataWarehouseInit', {
      runtime: lambda.Runtime.NODEJS_20_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.seconds(10),
      projectRoot: path.join(__dirname, 'init-db'),
      depsLockFilePath: path.join(__dirname, 'init-db', 'package-lock.json'),
      entry: path.join(__dirname, 'init-db', 'index.ts'),
      bundling: {
        commandHooks: {
          afterBundling: (inputDir: string, outputDir: string): string[] => [
            `cp ${inputDir}/global-bundle.pem ${outputDir}/global-bundle.pem`,
          ],
          // eslint-disable-next-line
          beforeBundling: (inputDir: string, outputDir: string): string[] => [],
          // eslint-disable-next-line
          beforeInstall: (inputDir: string, outputDir: string): string[] => [],
        },
      },
      environment: {
        MASTER_SECRET_NAME: dataWarehouse.secret!.secretName,
        WAREHOUSE_READ_SECRET_NAME: warehouseReadSecret.secretName,
        WAREHOUSE_WRITE_SECRET_NAME: this.warehouseWriteSecret.secretName
      },
      vpc: props.network.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      allowPublicSubnet: true,
      securityGroups: [dataWarehouseInitSecurityGroup]
    });
    dataWarehouse.secret!.grantRead(dataWarehouseInit);
    warehouseReadSecret.grantRead(dataWarehouseInit);
    this.warehouseWriteSecret.grantRead(dataWarehouseInit);
    this.dataWarehouseSecurityGroup.addIngressRule(
      dataWarehouseInitSecurityGroup,
      ec2.Port.tcp(5432),
      "Allow inbound from init function"
    );
  }
}
