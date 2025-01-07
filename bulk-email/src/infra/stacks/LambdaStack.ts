import { Fn, Stack, StackProps } from 'aws-cdk-lib';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LambdaIntegration } from 'aws-cdk-lib/aws-apigateway';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';
import { join } from 'path';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { ITopic, Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { ITable, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Bucket, EventType, IBucket } from 'aws-cdk-lib/aws-s3';
import { S3EventSourceV2 } from 'aws-cdk-lib/aws-lambda-event-sources';


export class LambdaStack extends Stack {
    public readonly processFileLambda: NodejsFunction;
    public readonly sendEmailLambda: NodejsFunction;
    public readonly notificationLambda: NodejsFunction;
    public readonly startStepFunctionLambda: NodejsFunction;
    public readonly startStepFunctionLambdaIntegration: LambdaIntegration;
    public readonly snsTopic: ITopic

    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        // Import the S3 bucket name and DynamoDB table name from other stacks
        const bucketName = Fn.importValue('BulkEmailS3BucketName');
        const tableName = Fn.importValue('BulkEmailDynamoDbTableName');

        // Create an IBucket and ITable objects using the bucket name and table name
        const uploadBucket: IBucket = Bucket.fromBucketName(this, 'BulkEmailS3Bucket', bucketName);
        const userTable: ITable = Table.fromTableName(this, 'BulkEmailDynamoDbTable', tableName);


        // Lambda to process the CSV file and store it in DynamoDB
        this.processFileLambda = new NodejsFunction(this, 'ProcessFileLambda', {
            runtime: Runtime.NODEJS_LATEST,
            handler: 'ProcessFile',
            entry: join(__dirname, '..', '..', 'services', 'emails', 'processFile.ts'),
            environment: {
                USER_TABLE: tableName,
                BUCKET_NAME: bucketName,
            },
        });

        // Grant permissions to the Lambda role
        this.processFileLambda.addToRolePolicy(new PolicyStatement({
            actions: ['s3:GetObject', 's3:ListBucket'],
            resources: [
                uploadBucket.bucketArn,
                `${uploadBucket.bucketArn}/*`,
            ],
        }))

        this.processFileLambda.addToRolePolicy(new PolicyStatement({
            actions: ['dynamodb:PutItem', 'dynamodb:BatchWriteItem'],
            resources: [userTable.tableArn],
        }));

        // Lambda to send emails
        this.sendEmailLambda = new NodejsFunction(this, 'SendEmailLambda', {
            runtime: Runtime.NODEJS_LATEST,
            handler: 'SendEmail',
            entry: join(__dirname, '..', '..', 'services', 'emails', 'sendEmail.ts'),
        });

        this.sendEmailLambda.addToRolePolicy(new PolicyStatement({
            actions: ['ses:SendEmail', 'ses:ListIdentities', 'ses:VerifyEmailIdentity', 'ses:VerifyDomainIdentity'],
            resources: ['*'],
        }));

        this.snsTopic = new Topic(this, 'EmailNotificationTopic', {
            displayName: 'Email Notification Topic',
        });

        // Lambda to send a notification about the results
        this.notificationLambda = new NodejsFunction(this, 'NotificationLambda', {
            runtime: Runtime.NODEJS_LATEST,
            handler: 'NotifyResults',
            entry: join(__dirname, '..', '..', 'services', 'emails', 'notifyResults.ts'),
            environment: {
                SNS_TOPIC_ARN: this.snsTopic.topicArn,
            },
        });

        // Grant the Lambda permission to publish to the SNS topic
        this.snsTopic.grantPublish(this.notificationLambda);

        this.snsTopic.addSubscription(new EmailSubscription('vikashbollam@gmail.com'))

        this.notificationLambda.addToRolePolicy(
            new PolicyStatement({
                actions: [
                    'ses:SendEmail',
                    'ses:SendRawEmail',
                    'sns:Publish'
                ],
                resources: ['*'],
            })
        );

        // Lambda to start the Step Function workflow
        this.startStepFunctionLambda = new NodejsFunction(this, 'StartStepFunctionLambda', {
            runtime: Runtime.NODEJS_LATEST,
            handler: 'StartStepFunction',
            entry: join(__dirname, '..', '..', 'services', 'emails', 'startStepFunction.ts'),
            environment: {
                STATE_MACHINE_ARN: StringParameter.valueForStringParameter(this, "/bulk-email/state-machine-arn")
            },
        });

        this.startStepFunctionLambda.addToRolePolicy(
            new PolicyStatement({
                actions: ['states:StartExecution'],
                resources: [StringParameter.valueForStringParameter(this, "/bulk-email/state-machine-arn")],
            })
        )

        this.startStepFunctionLambda.addEventSource(
            new S3EventSourceV2(uploadBucket, {
                events: [EventType.OBJECT_CREATED],
                filters: [{ suffix: ".csv" }]
            })
        );

        // API Gateway Lambda Integration to trigger the Step Function Lambda
        this.startStepFunctionLambdaIntegration = new LambdaIntegration(this.startStepFunctionLambda);
    }

}
