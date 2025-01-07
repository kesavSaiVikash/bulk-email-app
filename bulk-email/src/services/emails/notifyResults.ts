import { Handler } from 'aws-lambda';
import { SNS } from 'aws-sdk';

const sns = new SNS();

const sendNotification = async (message: string) => {

    const params = {
        Message: message,
        TopicArn: process.env.SNS_TOPIC_ARN,
    };

    const response = await sns.publish(params).promise();
};

export const NotifyResults: Handler = async (event) => {

    try {
        console.log('Received SNS event:', JSON.stringify(event, null, 2));

        const body = JSON.parse(event.Payload.body)

        const { successCount = 0, failedCount = 0 } = body || {};

        const notificationMessage = `Bulk email process completed: ${successCount} emails sent, ${failedCount} failed.`;

        await sendNotification(notificationMessage);

        return { statusCode: 200, body: JSON.stringify({ message: 'Notification sent successfully' }) };

    } catch (error) {
        console.error('Error notifying results:', error);

        return { statusCode: 500, body: JSON.stringify({ message: 'Error notifying results', error }) };
    }
};
