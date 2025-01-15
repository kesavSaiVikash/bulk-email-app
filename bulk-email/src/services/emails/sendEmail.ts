import { Handler } from "aws-lambda";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import { SQSClient, ReceiveMessageCommand, DeleteMessageBatchCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient, GetItemCommand, PutItemCommand, ReturnValue, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { backoff } from '../Utils';

const dynamoDbClient = new DynamoDBClient({ region: "ca-central-1" });
const sesClient = new SESClient({ region: "ca-central-1" });
const sqsClient = new SQSClient({ region: "ca-central-1" });

const sourceEmail = "no-reply@helpmarketing.ca";
const SQS_QUEUE_URL = process.env.QUEUE_URL!;

const RETRY_LIMIT = 1;

// Helper function to send email with retries
const sendEmailWithRetry = async (email: string, firstName: string, retries: number = 0) => {
    const params = {
        Destination: { ToAddresses: [email] },
        Message: {
            Body: {
                Text: { Data: `Hello ${firstName},\n\nWelcome to our service!` },
            },
            Subject: { Data: "Welcome Email" },
        },
        Source: sourceEmail,
    };

    console.log("SES params:", JSON.stringify(params));  // Check the SES request parameters

    try {
        // await sesClient.send(new SendEmailCommand(params));
        console.log(`Email sent to ${email}`);
        return { email, status: "Sent" };
    } catch (error) {
        console.error(`Error sending email to ${email}`, error);
        if (retries < RETRY_LIMIT) {
            console.log(`Retrying to send email to ${email}... Attempt ${retries + 1}`);
            return sendEmailWithRetry(email, firstName, retries + 1);
        }
        return { email, status: "Failed", error: error.message };
    }
};

const initializeStatsInDynamoDB = async (date: string) => {
    const params = {
        TableName: process.env.STATS_TABLE_NAME,
        Item: {
            id: { S: date },
            success: { N: "0" },
            failure: { N: "0" },
            version: { N: "0" },
        },
        ConditionExpression: "attribute_not_exists(id)",
    };

    try {
        await dynamoDbClient.send(new PutItemCommand(params));
        console.log(`Initialized stats for ${date} in DynamoDB.`);
    } catch (error) {
        if (error.name === "ConditionalCheckFailedException") {
            console.log(`Stats for ${date} already initialized.`);
        } else {
            console.error("Error initializing stats in DynamoDB:", error);
            throw error;
        }
    }
};


const MAX_RETRIES = 10; // Limit the number of retries to prevent infinite loops

const fetchStatsFromDynamoDB = async (date: string) => {
    const params = {
        TableName: process.env.STATS_TABLE_NAME,
        Key: {
            id: { S: date }, // Partition key
        },
        ProjectionExpression: "version", // Only fetch the version attribute
    };

    try {
        const data = await dynamoDbClient.send(new GetItemCommand(params));

        if (!data.Item) {
            console.log(`No stats found for ${date}. Initializing...`);
            await initializeStatsInDynamoDB(date);
            return 0; // Return initial version
        }

        return parseInt(data.Item.version.N, 10); // Return the current version as a number
    } catch (error) {
        console.error("Error fetching stats from DynamoDB:", error);
        throw error;
    }
};

const updateStatsInDynamoDB = async (
    date: string,
    success: number,
    failure: number,
    currentVersion: number, // Pass the version here
    retryCount = 0 // Keep track of retries
) => {
    const params = {
        TableName: process.env.STATS_TABLE_NAME,
        Key: {
            id: { S: date }, // Partition key
        },
        UpdateExpression:
            "ADD success :successValue, failure :failureValue SET version = version + :incrementValue",

        ConditionExpression: "attribute_exists(id) AND version = :currentVersion",

        ExpressionAttributeValues: {
            ":successValue": { N: success.toString() },
            ":failureValue": { N: failure.toString() },
            ":incrementValue": { N: "1" },
            ":currentVersion": { N: currentVersion.toString() },
        },

        ReturnValues: ReturnValue.ALL_NEW, // Return the updated values
    };

    try {
        const data = await dynamoDbClient.send(new UpdateItemCommand(params));
        console.log("Successfully updated stats with OCC in DynamoDB:", data);
        return data;
    } catch (error: any) {
        if (error.name === "ConditionalCheckFailedException") {
            console.warn(
                `Conflict detected. Retry #${retryCount + 1} with the latest version.`
            );

            if (retryCount < MAX_RETRIES) {

                const delay = backoff(retryCount); // Get the exponential backoff delay with jitter
                await new Promise((resolve) => setTimeout(resolve, delay));

                // Fetch the latest version
                const latestVersion = await fetchStatsFromDynamoDB(date);

                // Retry the update with the new version
                return updateStatsInDynamoDB(
                    date,
                    success,
                    failure,
                    latestVersion,
                    retryCount + 1
                );
            } else {
                console.error("Max retries reached. Could not update stats in DynamoDB.");
                throw error; // Throw the error after max retries
            }
        }
    }
};

export const SendEmail: Handler = async (event) => {
    let messagesProcessed = 0;
    let successCount = 0;
    let failedCount = 0;

    try {
        const currentDateTime = new Date().toISOString().split(":").slice(0, 2).join(":");

        // Loop through each record in the event (messages in the batch)
        for (const record of event.Records) {

            await new Promise((resolve) => setTimeout(resolve, 200));

            const { body, messageId, receiptHandle } = record;

            const messageBody = JSON.parse(body);

            const { email, firstName } = messageBody;

            console.log(`Processing message: ${messageId}`);

            if (!email || !firstName) {
                console.error("Invalid message format:", messageBody);
                continue; // Skip invalid messages
            }

            // Send the email with retry logic
            const result = await sendEmailWithRetry(email, firstName);

            if (result.status === "Sent") {
                successCount++;
                // Delete the message after processing
                await sqsClient.send(
                    new DeleteMessageBatchCommand({
                        QueueUrl: SQS_QUEUE_URL,
                        Entries: [{ Id: messageId, ReceiptHandle: receiptHandle }],
                    })
                );
                console.log(`Successfully deleted message: ${messageId}`);
            } else {
                failedCount++;
            }
            messagesProcessed++;
        }

        // Fetch the current version
        const currentVersion = await fetchStatsFromDynamoDB(currentDateTime);

        // Update the stats in DynamoDB
        await updateStatsInDynamoDB(currentDateTime, successCount, failedCount, currentVersion);

        // Return summary of email results
        console.log(`Total Messages Processed: ${messagesProcessed}`);

        return {
            statusCode: 200,
            body: JSON.stringify({ message: "Emails processed.", successCount, failedCount }),
        };
    } catch (error) {
        console.error("Error processing emails:", error);

        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Error processing emails.", error: error.message }),
        };
    }
};
