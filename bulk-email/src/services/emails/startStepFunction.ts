import { Handler } from "aws-lambda";
import { StepFunctions } from "aws-sdk";

const stepFunctions = new StepFunctions();

export const StartStepFunction: Handler = async (event) => {
    try {
        console.log("Received event: ", JSON.stringify(event));

        let bucket: string | undefined;

        let key: string | undefined;

        // Handle API Gateway event
        if ('body' in event) {
            console.log("Received event body:", event.body); // Log the raw body

            const body = JSON.parse(event.body);

            bucket = body.bucket;

            key = body.key;

            if (!bucket || !key) {
                throw new Error("Bucket and key information are required.");
            }

            console.log(`Received from API Gateway: Bucket: ${bucket}, Key: ${key}`);
        }

        // Handle S3 event
        else if ('Records' in event) {
            const record = event.Records[0];

            bucket = record.s3.bucket.name;

            key = record.s3.object.key;

            if (!bucket || !key) {
                throw new Error("Bucket and key information are required.");
            }

            console.log(`Received from S3 Event: Bucket: ${bucket}, Key: ${key}`);
        }

        const stateMachineArn = process.env.STATE_MACHINE_ARN!;

        const input = { bucket, key };

        const params = {
            stateMachineArn,
            input: JSON.stringify(input),
        };

        const result = await stepFunctions.startExecution(params).promise();

        console.log("Step Function started: ", result);

        return {
            statusCode: 200,
            body: JSON.stringify(result),
        };
    } catch (error) {
        console.error("Error starting Step Function: ", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: error.message }),
        };
    }
};
