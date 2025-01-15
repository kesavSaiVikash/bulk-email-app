import { Handler } from 'aws-lambda';
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';

const snsClient = new SNSClient({ region: 'ca-central-1' });
const dynamoDbClient = new DynamoDBClient({ region: 'ca-central-1' });

const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN!;
const STATS_TABLE_NAME = process.env.STATS_TABLE_NAME!;
const SUCCESS_ATTRIBUTE = 'success'; // Attribute name for success count
const FAILURE_ATTRIBUTE = 'failed'; // Attribute name for failure count

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const sendNotification = async (message: string) => {
    const params = {
        Message: message,
        TopicArn: SNS_TOPIC_ARN,
    };

    const response = await snsClient.send(new PublishCommand(params));
    return response;
};

export const NotifyResults: Handler = async () => {
    let successCount = 0;
    let failedCount = 0;

    try {

        const delayMs = 600000;
        console.log(`Delaying for ${delayMs}ms before fetching records...`);
        await delay(delayMs);

        // Perform a Scan operation on the table
        const scanParams = {
            TableName: STATS_TABLE_NAME,
        };

        const scanResponse = await dynamoDbClient.send(new ScanCommand(scanParams));

        const items = scanResponse.Items;
        if (!items || items.length === 0) {
            console.log('No records found in DynamoDB. Exiting...');
            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'No records found to process' }),
            };
        }

        console.log(`Fetched ${items.length} records from DynamoDB.`);

        // Loop through items to aggregate success and failure counts
        for (const item of items) {
            const success = parseInt(item[SUCCESS_ATTRIBUTE]?.N || '0');
            const failed = parseInt(item[FAILURE_ATTRIBUTE]?.N || '0');

            successCount += success;
            failedCount += failed;
        }

        // Send notification with results
        const notificationMessage = `Bulk email process completed: ${successCount} emails sent, ${failedCount} failed.`;
        const response = await sendNotification(notificationMessage);

        console.log('Notification sent:', response);

        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Notification sent successfully' }),
        };
    } catch (error) {
        console.error('Error notifying results:', error);

        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error notifying results', error: error.message }),
        };
    }
};
