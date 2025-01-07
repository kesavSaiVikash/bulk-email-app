import { S3 } from "@aws-sdk/client-s3";
import { DynamoDBClient, BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";
import { Handler } from "aws-lambda";
import { parse } from "csv-parse/sync";
import { Readable } from "stream";
import { streamToBuffer } from '../Utils';

const s3 = new S3();
const ddbClient = new DynamoDBClient();
const tableName = process.env.USER_TABLE!;
const BATCH_SIZE = 25;


export const ProcessFile: Handler = async (event) => {
    try {
        console.log("Received event: ", JSON.stringify(event));

        const { bucket, key } = event;

        if (!bucket || !key) {
            throw new Error("Bucket and key are required in the event body.");
        }

        const { Body } = await s3.getObject({ Bucket: bucket, Key: key });

        if (!Body) {
            throw new Error("Failed to retrieve file content from S3.");
        }

        const fileBuffer = await streamToBuffer(Body as Readable);

        // Parse CSV file
        const records = parse(fileBuffer.toString(), {
            columns: true,
            skip_empty_lines: true,
        });

        if (!records.length) {
            throw new Error("No records found in the CSV file.");
        }

        const batchWriteRequests = [];

        let batch = [];

        for (const { email, firstName } of records) {
            batch.push({
                PutRequest: {
                    Item: {
                        email: { S: email },
                        firstName: { S: firstName },
                    },
                },
            });

            if (batch.length === BATCH_SIZE) {
                batchWriteRequests.push(batch);
                batch = [];
            }
        }

        if (batch.length > 0) {
            batchWriteRequests.push(batch);
        }

        // Write to DynamoDB
        await Promise.all(
            batchWriteRequests.map((batchRequest) =>
                ddbClient.send(new BatchWriteItemCommand({ RequestItems: { [tableName]: batchRequest } }))
            )
        );

        console.log("Processed Records: ", JSON.stringify(records));

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "File processed successfully.",
                records,
            }),
        };

    } catch (error) {

        console.error("Error processing file: ", error);

        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Error processing file.",
                error: error.message,
            }),
        };
    }
};
