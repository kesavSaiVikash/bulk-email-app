import { Handler } from "aws-lambda";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";

const sesClient = new SESClient({ region: "ca-central-1" });
const sourceEmail = "no-reply@helpmarketing.ca";

export const SendEmail: Handler = async (event) => {
    try {
        console.log("Received event: ", JSON.stringify(event));

        const body = JSON.parse(event.Payload.body);

        const records = body.records;

        if (!records || !Array.isArray(records)) {
            throw new Error("Invalid email data provided.");
        }

        console.log(`Sending emails to ${records.length} recipients`);

        const emailResults = await Promise.allSettled(
            records.map(async ({ email, firstName }: { email: string; firstName: string }) => {
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

                try {
                    await sesClient.send(new SendEmailCommand(params));
                    return { email, status: "Sent" };
                } catch (error) {
                    console.error("SES Error:", error);
                    return { email, status: "Failed", error: error.message };
                }
            })
        );

        const successCount = emailResults.filter((result: any) => result.value.status === "Sent").length;
        const failedCount = emailResults.filter((result: any) => result.value.status === "Failed").length;

        console.log(`Emails sent: ${successCount}, failed: ${failedCount}`);

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "Emails processed.",
                successCount,
                failedCount,
            }),
        };
    } catch (error) {
        console.error("Error sending emails:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Error sending emails.",
                error: error.message,
            }),
        };
    }
}
