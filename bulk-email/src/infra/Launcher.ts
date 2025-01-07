import { App } from 'aws-cdk-lib';
import { DataStack } from './stacks/DataStack';
import { LambdaStack } from './stacks/LambdaStack';
import { StepFunctionsStack } from './stacks/StepFunctionsStack';
import { ApiStack } from './stacks/ApiStack';

const app = new App();

new DataStack(app, "BulkEmailDataStack");

const lambdaStack = new LambdaStack(app, "BulkEmailLambdaStack");

new StepFunctionsStack(app, "BulkEmailStepFunctionsStack", {
    processFileLambda: lambdaStack.processFileLambda,
    sendEmailLambda: lambdaStack.sendEmailLambda,
    notificationLambda: lambdaStack.notificationLambda,
});

new ApiStack(app, "BulkEmailApiStack", {
    startStepFunctionLambdaIntegration: lambdaStack.startStepFunctionLambdaIntegration,
});
