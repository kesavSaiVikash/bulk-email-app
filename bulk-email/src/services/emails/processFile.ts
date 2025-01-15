import { AthenaClient, StartQueryExecutionCommand, GetQueryResultsCommand, GetQueryExecutionCommand } from "@aws-sdk/client-athena";
import { SQSClient, SendMessageBatchCommand } from "@aws-sdk/client-sqs";
import { Handler } from "aws-lambda";
import { backoff } from '../Utils';

const athenaClient = new AthenaClient({ region: "ca-central-1" });
const sqsClient = new SQSClient({ region: "ca-central-1" });

const SQS_QUEUE_URL = process.env.QUEUE_URL!;
const ATHENA_DATABASE = process.env.ATHENA_DATABASE!;
const ATHENA_TABLE = process.env.ATHENA_TABLE!;
const ATHENA_BUCKET_NAME = process.env.ATHENA_BUCKET_NAME!;

const BATCH_SIZE = 10;

// Helper function to send a batch to SQS
const sendBatchToSQS = async (batch: { firstName: string; email: string }[]) => {

    const entries = batch.map((item, index) => ({
        Id: index.toString(),
        MessageBody: JSON.stringify(item),
    }));

    console.log("entries: " + JSON.stringify(entries))

    try {
        const result = await sqsClient.send(
            new SendMessageBatchCommand({
                QueueUrl: SQS_QUEUE_URL,
                Entries: entries,
            })
        );

        if (result.Failed && result.Failed.length > 0) {
            console.error("Failed messages: ", result.Failed);
        }

        console.log("Batch sent to SQS successfully:", result);
    } catch (err) {
        console.error("Error sending batch to SQS:", err);
    }
};

// Retry helper function for Athena query execution
const retryAthenaQueryExecution = async (queryParams: any) => {
    let attempts = 0;
    const maxAttempts = 5;
    let queryExecutionResult;

    while (attempts < maxAttempts) {
        try {
            queryExecutionResult = await athenaClient.send(new StartQueryExecutionCommand(queryParams));
            return queryExecutionResult;
        } catch (error) {
            if (error.name === "TooManyRequestsException") {
                console.log(`TooManyRequestsException encountered. Retrying... (Attempt ${attempts + 1}/${maxAttempts})`);
                const delay = backoff(attempts); // Use exponential backoff with jitter
                await new Promise(resolve => setTimeout(resolve, delay));
                attempts++;
            } else {
                throw error;
            }
        }
    }
    throw new Error("Max retries reached for query execution.");
};

// Lambda Handler
export const ProcessFile: Handler = async (event) => {
    try {
        const { start, end } = event;

        // Construct Athena query
        const query =
            `WITH PaginatedResults AS (
            SELECT 
                firstName, 
                email, 
                ROW_NUMBER() OVER () AS row_num
            FROM "${ATHENA_TABLE}"
        )
        SELECT firstName, email
        FROM PaginatedResults
        WHERE row_num BETWEEN ${start} AND ${end};`;

        const queryParams = {
            QueryString: query,
            QueryExecutionContext: { Database: ATHENA_DATABASE },
            ResultConfiguration: {
                OutputLocation: `s3://${ATHENA_BUCKET_NAME}/athena-results/`,
            },
        };

        // Retry Athena query execution
        const queryExecutionResult = await retryAthenaQueryExecution(queryParams);

        console.log("Athena query execution response:", queryExecutionResult);

        const queryExecutionId = queryExecutionResult.QueryExecutionId;

        if (!queryExecutionId) {
            throw new Error("Failed to start Athena query.");
        }

        let queryState = "RUNNING";
        let attempts = 0;
        let nextToken: string | undefined = undefined;

        // Poll Athena until query completes 
        while (queryState === "RUNNING" || queryState === "QUEUED") {

            const delay = backoff(attempts); // Get the exponential backoff delay with jitter
            await new Promise((resolve) => setTimeout(resolve, delay));

            const statusResponse = await athenaClient.send(
                new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId })
            );

            queryState = statusResponse.QueryExecution?.Status?.State || "UNKNOWN";

            attempts++;
        }

        if (queryState !== "SUCCEEDED") {
            throw new Error(`Query failed with status: ${queryState}`);
        }

        let isHeaderProcessed = false;

        // Fetch results in paginated manner
        do {

            const results = await athenaClient.send(
                new GetQueryResultsCommand({
                    QueryExecutionId: queryExecutionId,
                    NextToken: nextToken,
                })
            );

            const rows = results.ResultSet?.Rows || [];

            // const formattedRows = rows.map((row) => ({
            //     email: row.Data?.[1]?.VarCharValue || "",
            //     firstName: row.Data?.[0]?.VarCharValue || "",
            // }));

            // Only skip the header row once (for the first result set)
            const formattedRows = rows
                .slice(isHeaderProcessed ? 0 : 1)
                .map((row) => ({
                    email: row.Data?.[1]?.VarCharValue || "",
                    firstName: row.Data?.[0]?.VarCharValue || "",
                }));

            isHeaderProcessed = true; // Mark header as processed

            // Send data to SQS in batches
            for (let i = 0; i < formattedRows.length; i += BATCH_SIZE) {

                const batch = formattedRows.slice(i, i + BATCH_SIZE);

                await sendBatchToSQS(batch);
            }

            nextToken = results.NextToken;
        } while (nextToken);

        return {
            statusCode: 200,
            body: JSON.stringify({ message: "File processed successfully." }),
        };

    } catch (error) {
        console.error("Error processing file: ", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Error processing file.", error: error.message }),
        };
    }
};