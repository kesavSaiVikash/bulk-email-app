import { CfnOutput, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib'
import { Bucket, HttpMethods, IBucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { getSuffixFromStack } from '../Utils';
import { AttributeType, BillingMode, ITable, Table } from 'aws-cdk-lib/aws-dynamodb';


export class DataStack extends Stack {
    public readonly uploadBucket: IBucket;
    public readonly userTable: ITable;

    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const suffix = getSuffixFromStack(this);

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

        this.userTable = new Table(this, 'BulkEmailUserTable', {
            partitionKey: {
                name: 'email', type: AttributeType.STRING,
            },
            billingMode: BillingMode.PAY_PER_REQUEST,
            removalPolicy: RemovalPolicy.DESTROY,
        })

        new CfnOutput(this, 'BulkEmailS3BucketNameOutput', {
            value: this.uploadBucket.bucketName,
            exportName: 'BulkEmailS3BucketName',
        });

        new CfnOutput(this, 'BulkEmailDynamoDbTableNameOutput', {
            value: this.userTable.tableName,
            exportName: 'BulkEmailDynamoDbTableName',
        });
    }
}