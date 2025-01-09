import { AthenaClient, StartQueryExecutionCommand, GetQueryResultsCommand, GetQueryExecutionCommand } from "@aws-sdk/client-athena";
import { SQSClient, SendMessageBatchCommand } from "@aws-sdk/client-sqs";
import { Handler } from "aws-lambda";

const athenaClient = new AthenaClient({ region: "ca-central-1" });
const sqsClient = new SQSClient({ region: "ca-central-1" });

const SQS_QUEUE_URL = process.env.QUEUE_URL!;
const BATCH_SIZE = 10;
const POLLING_DELAY = 5000;
const ATHENA_DATABASE = process.env.ATHENA_DATABASE!;
const ATHENA_TABLE = process.env.ATHENA_TABLE!;
const ATHENA_BUCKET_NAME = process.env.ATHENA_BUCKET_NAME!;

// Helper function to send a batch to SQS
const sendBatchToSQS = async (batch: { firstName: string; email: string }[]) => {
    const entries = batch.map((item, index) => ({
        Id: index.toString(),
        MessageBody: JSON.stringify(item),
    }));

    console.log("entries: " + entries)

    try {
        const result = await sqsClient.send(
            new SendMessageBatchCommand({
                QueueUrl: SQS_QUEUE_URL,
                Entries: entries,
            })
        );
        console.log("Batch sent to SQS successfully:", result);
    } catch (err) {
        console.error("Error sending batch to SQS:", err);
    }
};

// Lambda Handler
export const ProcessFile: Handler = async (event) => {
    try {
        const { bucket, key } = event;
        if (!bucket || !key) {
            throw new Error("Bucket and key are required in the event body.");
        }

        // Construct Athena query
        const query = `SELECT firstName, email FROM "${ATHENA_TABLE}"`;
        console.log("Generated Athena Query: ", query);

        // Start Athena query execution
        const queryParams = {
            QueryString: query,
            QueryExecutionContext: { Database: ATHENA_DATABASE },
            ResultConfiguration: {
                OutputLocation: `s3://${ATHENA_BUCKET_NAME}/athena-results/`,
            },
        };
        const queryExecutionResult = await athenaClient.send(new StartQueryExecutionCommand(queryParams));
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
            const delay = Math.min(1000 * Math.pow(2, attempts), 60000);
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

        console.log("Query completed successfully. Fetching results...");

        // Fetch results in paginated manner
        do {
            const results = await athenaClient.send(
                new GetQueryResultsCommand({
                    QueryExecutionId: queryExecutionId,
                    NextToken: nextToken,
                })
            );

            const rows = results.ResultSet?.Rows || [];

            console.log("rows: " + JSON.stringify(rows))

            const formattedRows = rows.slice(1).map((row) => ({
                email: row.Data?.[1]?.VarCharValue || "",
                firstName: row.Data?.[0]?.VarCharValue || "",
            }));

            // Send data to SQS in batches
            for (let i = 0; i < formattedRows.length; i += BATCH_SIZE) {
                const batch = formattedRows.slice(i, i + BATCH_SIZE);
                console.log("batch: " + JSON.stringify(batch))
                const res = await sendBatchToSQS(batch);
                console.log("res : " + res)
            }

            nextToken = results.NextToken;
        } while (nextToken);

        console.log("All query results sent to SQS successfully.");
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
