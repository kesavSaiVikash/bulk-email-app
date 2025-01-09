import { Duration, Fn, Stack, StackProps } from 'aws-cdk-lib';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LambdaIntegration } from 'aws-cdk-lib/aws-apigateway';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';
import { join } from 'path';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { ITopic, Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { Bucket, EventType, IBucket } from 'aws-cdk-lib/aws-s3';
import { S3EventSourceV2, SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Queue, IQueue } from 'aws-cdk-lib/aws-sqs';



export class LambdaStack extends Stack {
    public readonly processFileLambda: NodejsFunction;
    public readonly sendEmailLambda: NodejsFunction;
    public readonly notificationLambda: NodejsFunction;
    public readonly startStepFunctionLambda: NodejsFunction;
    public readonly startStepFunctionLambdaIntegration: LambdaIntegration;
    public readonly snsTopic: ITopic
    public readonly emailQueue: IQueue;

    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const bucketName = Fn.importValue('BulkEmailS3BucketName');
        const athenaResultsBucketName = Fn.importValue('AthenaQueryResultsBucketName');

        const uploadBucket: IBucket = Bucket.fromBucketName(this, 'BulkEmailS3Bucket', bucketName);
        const athenaResultsBucket: IBucket = Bucket.fromBucketName(this, 'AthenaQueryResultsBucket', athenaResultsBucketName);

        // Athena Database and Table reference from Athena Stack
        const athenaDatabaseName = Fn.importValue('BulkEmailAthenaDbName');
        const athenaTableName = Fn.importValue('BulkEmailAthenaTableName');

        this.emailQueue = new Queue(this, 'EmailQueue', {
            queueName: 'bulk-email-queue',
            visibilityTimeout: Duration.seconds(300),
        });

        this.emailQueue.addToResourcePolicy(
            new PolicyStatement({
                principals: [new ServicePrincipal("lambda.amazonaws.com")],
                actions: ["sqs:SendMessage", "sqs:ReceiveMessage"],
                resources: [this.emailQueue.queueArn],
            })
        );

        this.processFileLambda = new NodejsFunction(this, 'ProcessFileLambda', {
            runtime: Runtime.NODEJS_LATEST,
            handler: 'ProcessFile',
            entry: join(__dirname, '..', '..', 'services', 'emails', 'processFile.ts'),
            environment: {
                BUCKET_NAME: bucketName,
                QUEUE_URL: this.emailQueue.queueUrl,
                ATHENA_DATABASE: athenaDatabaseName,
                ATHENA_TABLE: athenaTableName,
                ATHENA_BUCKET_NAME: athenaResultsBucketName
            },
            timeout: Duration.minutes(2)
        });

        // Grant Lambda permission to send messages to SQS
        this.processFileLambda.addToRolePolicy(new PolicyStatement({
            actions: ['sqs:SendMessage', 'sqs:SendMessageBatch'],
            resources: [this.emailQueue.queueArn],
        }));


        // Grant permissions to Lambda to query Athena
        this.processFileLambda.addToRolePolicy(new PolicyStatement({
            actions: ['athena:StartQueryExecution',
                'athena:GetQueryResults',
                'athena:ListQueryExecutions',
                'athena:StopQueryExecution',
                'athena:GetQueryExecution',

                'glue:GetDatabase',
                'glue:GetTable',
                'glue:GetTableVersion'],
            resources: [
                `arn:aws:athena:${this.region}:${this.account}:workgroup/primary`,

                `arn:aws:glue:${this.region}:${this.account}:catalog`,
                `arn:aws:glue:${this.region}:${this.account}:database/*`,
                `arn:aws:glue:${this.region}:${this.account}:table/*`
            ],
        }));

        // Grant permissions to the Lambda role
        this.processFileLambda.addToRolePolicy(new PolicyStatement({
            actions: ['s3:GetObject', "s3:ListBucket"],
            resources: [
                uploadBucket.bucketArn,
                `${uploadBucket.bucketArn}/*`,
            ],
        }))

        athenaResultsBucket.grantReadWrite(this.processFileLambda)

        // Lambda to send emails
        this.sendEmailLambda = new NodejsFunction(this, 'SendEmailLambda', {
            runtime: Runtime.NODEJS_LATEST,
            handler: 'SendEmail',
            entry: join(__dirname, '..', '..', 'services', 'emails', 'sendEmail.ts'),
            environment: {
                QUEUE_URL: this.emailQueue.queueUrl,
            },
            timeout: Duration.minutes(2)
        });

        this.sendEmailLambda.addToRolePolicy(new PolicyStatement({
            actions: ['ses:SendEmail', 'ses:ListIdentities', 'ses:VerifyEmailIdentity', 'ses:VerifyDomainIdentity'],
            resources: ['*'],
        }));

        this.sendEmailLambda.addToRolePolicy(new PolicyStatement({
            actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
            resources: [this.emailQueue.queueArn],
        }));

        // this.sendEmailLambda.addEventSource(new SqsEventSource(this.emailQueue));

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
