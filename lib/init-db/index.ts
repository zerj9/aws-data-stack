import { Handler } from 'aws-lambda';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { Client } from 'pg';
import * as fs from 'fs';


const masterSecretName = process.env.MASTER_SECRET_NAME; // Environment variable for the secret name
const warehouseReadSecretName = process.env.WAREHOUSE_READ_SECRET_NAME;
const warehouseWriteSecretName = process.env.WAREHOUSE_WRITE_SECRET_NAME;
const secretsManager = new SecretsManagerClient();

// eslint-disable-next-line
export const handler: Handler = async (event, context) => {
//export async function handler(/*_event: any, _context: any*/) {
  try {
    // Retrieve PostgreSQL credentials from AWS Secrets Manager (No caching between runs)
    const getMasterSecretCommand = new GetSecretValueCommand({ SecretId: masterSecretName });
    const getMasterSecretResponse = await secretsManager.send(getMasterSecretCommand);
    const masterSecret = JSON.parse(getMasterSecretResponse.SecretString!);

   // Retrieve warehouse read-only credentials from AWS Secrets Manager
    const getWarehouseReadSecretCommand = new GetSecretValueCommand({ SecretId: warehouseReadSecretName });
    const getWarehouseReadSecretResponse = await secretsManager.send(getWarehouseReadSecretCommand);
    const warehouseReadSecret = JSON.parse(getWarehouseReadSecretResponse.SecretString!);

    // Retrieve warehouse write credentials from AWS Secrets Manager
    const getWarehouseWriteSecretCommand = new GetSecretValueCommand({ SecretId: warehouseWriteSecretName });
    const getWarehouseWriteSecretResponse = await secretsManager.send(getWarehouseWriteSecretCommand);
    const warehouseWriteSecret = JSON.parse(getWarehouseWriteSecretResponse.SecretString!);

   // Connect to PostgreSQL database
    const postgresClient = new Client({
      user: masterSecret.username,
      password: masterSecret.password,
      host: masterSecret.host,
      port: masterSecret.port,
      database: "postgres",
      ssl: {
        ca: fs.readFileSync('./global-bundle.pem').toString(),
      }
    });
    await postgresClient.connect()

    const dbExistsResult = await postgresClient.query("SELECT 1 FROM pg_database WHERE datname='warehouse';");
    if (dbExistsResult.rows.length === 0) {
      await postgresClient.query('CREATE DATABASE warehouse')
    }
    await postgresClient.end()

    // Connect to warehouse database
    const warehouseClient = new Client({
      user: masterSecret.username,
      password: masterSecret.password,
      host: masterSecret.host,
      port: masterSecret.port,
      database: "warehouse",
      ssl: {
        ca: fs.readFileSync('./global-bundle.pem').toString(),
      }
    });
    await warehouseClient.connect()
    await warehouseClient.query('CREATE SCHEMA IF NOT EXISTS dataset;');
    await warehouseClient.query('ALTER DATABASE warehouse SET search_path="dataset";');

    // Create warehouse write user
    const writeUserExistsResult = await warehouseClient.query(`SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='${warehouseWriteSecret.username}';`);
    if (writeUserExistsResult.rows.length === 0) {
      // Create a user 'warehouse_write' and grant write access to the 'warehouse' database
      await warehouseClient.query(`CREATE USER ${warehouseWriteSecret.username} WITH ENCRYPTED PASSWORD '${warehouseWriteSecret.password}';`);
    }
    await warehouseClient.query(`GRANT CONNECT ON DATABASE warehouse TO ${warehouseWriteSecret.username};`);
    await warehouseClient.query(`GRANT USAGE ON SCHEMA dataset TO ${warehouseWriteSecret.username};`);
    await warehouseClient.query(`GRANT CREATE ON SCHEMA dataset TO ${warehouseWriteSecret.username};`);
    await warehouseClient.query(`ALTER DEFAULT PRIVILEGES IN SCHEMA dataset GRANT INSERT, UPDATE, DELETE ON TABLES TO ${warehouseWriteSecret.username};`);

    // Create read only warehouse user
    const readUserExistsResult = await warehouseClient.query(`SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='${warehouseReadSecret.username}';`);
    if (readUserExistsResult.rows.length === 0) {
      // Create a user 'warehouse_read' and grant read-only access to the 'warehouse' database
      await warehouseClient.query(`CREATE USER ${warehouseReadSecret.username} WITH ENCRYPTED PASSWORD '${warehouseReadSecret.password}';`);
    }
    await warehouseClient.query(`GRANT CONNECT ON DATABASE warehouse TO ${warehouseReadSecret.username};`);
    await warehouseClient.query(`GRANT USAGE ON SCHEMA dataset TO ${warehouseReadSecret.username};`);
    await warehouseClient.query(`GRANT SELECT ON ALL TABLES IN SCHEMA dataset TO ${warehouseReadSecret.username};`);
    // Allow root user to grant access to tables created using warehouseWrite user
    await warehouseClient.query(`GRANT ${warehouseWriteSecret.username} TO ${masterSecret.username};`);
    await warehouseClient.query(`ALTER DEFAULT PRIVILEGES FOR USER ${warehouseWriteSecret.username} IN SCHEMA dataset GRANT SELECT ON TABLES TO ${warehouseReadSecret.username};`);

    warehouseClient.end();

    return {
      statusCode: 200,
      body: 'Schema and user created successfully',
    };
  } catch (error) {
    console.error('Error:', error);
    return {
      statusCode: 500,
      body: 'Error creating schema and user',
    };
  }
}
