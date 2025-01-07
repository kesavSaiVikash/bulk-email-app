import { Stack, StackProps } from 'aws-cdk-lib';
import { LambdaIntegration } from 'aws-cdk-lib/aws-apigateway';
import { RestApi } from 'aws-cdk-lib/aws-apigateway';
import { Construct } from 'constructs';

interface ApiStackProps extends StackProps {
    startStepFunctionLambdaIntegration: LambdaIntegration;
}

export class ApiStack extends Stack {
    constructor(scope: Construct, id: string, props: ApiStackProps) {
        super(scope, id, props);

        const api = new RestApi(this, 'BulkEmailApi', {
            restApiName: 'BulkEmailService',
            description: 'API to trigger bulk email sending via Step Function',
        });

        const startFunctionResource = api.root.addResource('start-function');

        startFunctionResource.addMethod('POST', props.startStepFunctionLambdaIntegration);
    }
}
