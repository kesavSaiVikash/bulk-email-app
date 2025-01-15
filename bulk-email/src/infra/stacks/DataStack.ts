import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib'
import { Bucket, HttpMethods, IBucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { getSuffixFromStack } from '../Utils';
import {
    AttributeType,
    BillingMode,
    Table as DynamoDbTable,
    ITable,
} from "aws-cdk-lib/aws-dynamodb";

export class DataStack extends Stack {
    public readonly uploadBucket: IBucket;
    public readonly athenaQueryResultsBucket: IBucket
    public readonly statsTable: DynamoDbTable;

    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const suffix = getSuffixFromStack(this);

        this.statsTable = new DynamoDbTable(this, "BulkEmailStatsTable", {
            partitionKey: {
                name: "id",
                type: AttributeType.STRING,
            },
            tableName: `BulkEmailStatsTable-${suffix}`,
            billingMode: BillingMode.PAY_PER_REQUEST
        });

        this.statsTable.addGlobalSecondaryIndex({
            indexName: "version-index",
            partitionKey: {
                name: "id",
                type: AttributeType.STRING,
            },
            sortKey: {
                name: "version",
                type: AttributeType.NUMBER,
            },
        });

        this.uploadBucket = new Bucket(this, "BulkEmail", {
            bucketName: `bulk-email-files-${suffix}`,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            cors: [
                {
                    allowedMethods: [HttpMethods.HEAD, HttpMethods.GET, HttpMethods.PUT],
                    allowedOrigins: ["*"],
                    allowedHeaders: ["*"],
                },
            ],
            blockPublicAccess: {
                blockPublicAcls: false,
                blockPublicPolicy: false,
                ignorePublicAcls: false,
                restrictPublicBuckets: false,
            },
        });

        // Create an S3 bucket for Athena query results
        this.athenaQueryResultsBucket = new Bucket(this, 'AthenaQueryResultsBucket', {
            bucketName: `athena-query-results-${suffix}`,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            lifecycleRules: [
                {
                    enabled: true,
                    expiration: Duration.days(365),
                    prefix: 'athena-results/',
                },
            ],
            cors: [
                {
                    allowedMethods: [HttpMethods.HEAD, HttpMethods.GET, HttpMethods.PUT],
                    allowedOrigins: ["*"],
                    allowedHeaders: ["*"],
                },
            ],
            blockPublicAccess: {
                blockPublicAcls: false,
                blockPublicPolicy: false,
                ignorePublicAcls: false,
                restrictPublicBuckets: false,
            },
        });

        new CfnOutput(this, 'BulkEmailS3BucketNameOutput', {
            value: this.uploadBucket.bucketName,
            exportName: 'BulkEmailS3BucketName',
        });

        new CfnOutput(this, 'BulkEmailStatsTableNameOutput', {
            value: this.statsTable.tableName,
            exportName: 'BulkEmailStatsTableName',
        });

        new CfnOutput(this, 'AthenaQueryResultsBucketNameOutput', {
            value: this.athenaQueryResultsBucket.bucketName,
            exportName: 'AthenaQueryResultsBucketName',
        });
    }
}