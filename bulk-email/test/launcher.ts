import axios from 'axios';
import * as fs from 'fs';
import FormData from 'form-data';

async function sendPostRequest() {
    const form = new FormData();
    form.append('file', fs.createReadStream('/Users/kesavsaivikashbollam/Desktop/aws-serverless-course/bulk-email-app/bulk-email/test/test.csv')); // Replace with your file path

    try {
        const response = await axios.post(
            'https://xdyf3wbmyk.execute-api.ca-central-1.amazonaws.com/prod/start-function', // Your API endpoint
            form,
            {
                headers: {
                    ...form.getHeaders(), // Add form headers
                },
            }
        );
        console.log('Response:', response.data);
    } catch (error) {
        console.error('Error:', error);
    }
}

sendPostRequest();
