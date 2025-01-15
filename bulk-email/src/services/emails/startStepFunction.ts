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

        // Define an array of start and end values to process
        const chunks =
        {
            "data": [
                { "start": 0, "end": 1000 },
                { "start": 1001, "end": 2000 },
                { "start": 2001, "end": 3000 },
                { "start": 3001, "end": 4000 },
                { "start": 4001, "end": 5000 },
                { "start": 5001, "end": 6000 },
                { "start": 6001, "end": 7000 },
                { "start": 7001, "end": 8000 },
                { "start": 8001, "end": 9000 },
                { "start": 9001, "end": 10000 },
                { "start": 10001, "end": 11000 },
                { "start": 11001, "end": 12000 },
                { "start": 12001, "end": 13000 },
                { "start": 13001, "end": 14000 },
                { "start": 14001, "end": 15000 },
                { "start": 15001, "end": 16000 },
                { "start": 16001, "end": 17000 },
                { "start": 17001, "end": 18000 },
                { "start": 18001, "end": 19000 },
                { "start": 19001, "end": 20000 },
                { "start": 20001, "end": 21000 },
                { "start": 21001, "end": 22000 },
                { "start": 22001, "end": 23000 },
                { "start": 23001, "end": 24000 },
                { "start": 24001, "end": 25000 },
                { "start": 25001, "end": 26000 },
                { "start": 26001, "end": 27000 },
                { "start": 27001, "end": 28000 },
                { "start": 28001, "end": 29000 },
                { "start": 29001, "end": 30000 },
                { "start": 30001, "end": 31000 },
                { "start": 31001, "end": 32000 },
                { "start": 32001, "end": 33000 },
                { "start": 33001, "end": 34000 },
                { "start": 34001, "end": 35000 },
                { "start": 35001, "end": 36000 },
                { "start": 36001, "end": 37000 },
                { "start": 37001, "end": 38000 },
                { "start": 38001, "end": 39000 },
                { "start": 39001, "end": 40000 },
                { "start": 40001, "end": 41000 },
                { "start": 41001, "end": 42000 },
                { "start": 42001, "end": 43000 },
                { "start": 43001, "end": 44000 },
                { "start": 44001, "end": 45000 },
                { "start": 45001, "end": 46000 },
                { "start": 46001, "end": 47000 },
                { "start": 47001, "end": 48000 },
                { "start": 48001, "end": 49000 },
                { "start": 49001, "end": 50000 },
                { "start": 50001, "end": 51000 },
                { "start": 51001, "end": 52000 },
                { "start": 52001, "end": 53000 },
                { "start": 53001, "end": 54000 },
                { "start": 54001, "end": 55000 },
                { "start": 55001, "end": 56000 },
                { "start": 56001, "end": 57000 },
                { "start": 57001, "end": 58000 },
                { "start": 58001, "end": 59000 },
                { "start": 59001, "end": 60000 },
                { "start": 60001, "end": 61000 },
                { "start": 61001, "end": 62000 },
                { "start": 62001, "end": 63000 },
                { "start": 63001, "end": 64000 },
                { "start": 64001, "end": 65000 },
                { "start": 65001, "end": 66000 },
                { "start": 66001, "end": 67000 },
                { "start": 67001, "end": 68000 },
                { "start": 68001, "end": 69000 },
                { "start": 69001, "end": 70000 },
                { "start": 70001, "end": 71000 },
                { "start": 71001, "end": 72000 },
                { "start": 72001, "end": 73000 },
                { "start": 73001, "end": 74000 },
                { "start": 74001, "end": 75000 },
                { "start": 75001, "end": 76000 },
                { "start": 76001, "end": 77000 },
                { "start": 77001, "end": 78000 },
                { "start": 78001, "end": 79000 },
                { "start": 79001, "end": 80000 },
                { "start": 80001, "end": 81000 },
                { "start": 81001, "end": 82000 },
                { "start": 82001, "end": 83000 },
                { "start": 83001, "end": 84000 },
                { "start": 84001, "end": 85000 },
                { "start": 85001, "end": 86000 },
                { "start": 86001, "end": 87000 },
                { "start": 87001, "end": 88000 },
                { "start": 88001, "end": 89000 },
                { "start": 89001, "end": 90000 },
                { "start": 90001, "end": 91000 },
                { "start": 91001, "end": 92000 },
                { "start": 92001, "end": 93000 },
                { "start": 93001, "end": 94000 },
                { "start": 94001, "end": 95000 },
                { "start": 95001, "end": 96000 },
                { "start": 96001, "end": 97000 },
                { "start": 97001, "end": 98000 },
                { "start": 98001, "end": 99000 },
                { "start": 99001, "end": 100000 }
            ]
        }

        const input = { bucket, key, chunks };

        const params = {
            stateMachineArn,
            input: JSON.stringify(input),
        };

        const result = await stepFunctions.startExecution(params).promise();

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
