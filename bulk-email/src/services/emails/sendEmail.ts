import { Handler } from "aws-lambda";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, DeleteMessageBatchCommand } from "@aws-sdk/client-sqs";

const sesClient = new SESClient({ region: "ca-central-1" });
const sqsClient = new SQSClient({ region: "ca-central-1" });

const sourceEmail = "no-reply@helpmarketing.ca";
const SQS_QUEUE_URL = process.env.QUEUE_URL!;

const MAX_MESSAGES = 10; // Maximum number of messages to fetch from SQS in one batch
const VISIBILITY_TIMEOUT = 60; // Timeout duration for visibility of messages while processing
const RETRY_LIMIT = 1; // Retry limit for failed email sends

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

// Lambda Handler to process messages from SQS and send emails
export const SendEmail: Handler = async () => {
    let messagesProcessed = 0;
    let successCount = 0;
    let failedCount = 0;

    try {
        while (true) {
            const sqsResponse = await sqsClient.send(
                new ReceiveMessageCommand({
                    QueueUrl: SQS_QUEUE_URL,
                    MaxNumberOfMessages: MAX_MESSAGES,
                    WaitTimeSeconds: 10, // Enable long polling to reduce cost
                    VisibilityTimeout: VISIBILITY_TIMEOUT, // Adjust to Lambda processing time
                })
            );

            const messages = sqsResponse.Messages;

            if (!messages) {
                console.log("No messages found. Exiting...");
                break; // Exit if no messages left in queue
            }

            console.log(`Fetched ${messages.length} messages from SQS.`);

            // Array to hold receipt handles for successful messages
            const successfulMessages: any[] = [];

            // Process messages concurrently using Promise.all
            const emailResults = await Promise.allSettled(
                messages.map(async (message) => {
                    const body = JSON.parse(message.Body || "{}");
                    const { email, firstName } = body;

                    console.log(email, firstName)

                    if (!email || !firstName) {
                        console.error("Invalid message format:", body);
                        throw new Error("Invalid message format.");
                    }

                    const result = await sendEmailWithRetry(email, firstName);

                    // If email was sent successfully, add receipt handle to the array
                    if (result.status === "Sent") {
                        successfulMessages.push({
                            Id: message.MessageId!,
                            ReceiptHandle: message.ReceiptHandle!,
                        });
                    }

                    // Delete message from SQS if email was sent successfully
                    // if (result.status === "Sent") {
                    // await sqsClient.send(
                    //     new DeleteMessageCommand({
                    //         QueueUrl: SQS_QUEUE_URL,
                    //         ReceiptHandle: message.ReceiptHandle!,
                    //     })
                    // );
                    // }

                    // Update success or failure count
                    if (result.status === "Sent") {
                        successCount++;
                    } else {
                        failedCount++;
                    }

                    return result;
                })
            );

            // Delete the successfully processed messages in batch
            if (successfulMessages.length > 0) {
                await sqsClient.send(
                    new DeleteMessageBatchCommand({
                        QueueUrl: SQS_QUEUE_URL,
                        Entries: successfulMessages,
                    })
                );
                console.log(`Deleted ${successfulMessages.length} messages from SQS.`);
            }

            // Log the results for the current batch
            console.log(`Processed ${messages.length} messages. Success: ${successCount}, Failed: ${failedCount}`);

            // Count messages processed in this batch
            messagesProcessed += messages.length;
        }

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
