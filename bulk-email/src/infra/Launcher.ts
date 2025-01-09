import { App } from 'aws-cdk-lib';
import { DataStack } from './stacks/DataStack';
import { LambdaStack } from './stacks/LambdaStack';
import { StepFunctionsStack } from './stacks/StepFunctionsStack';
import { ApiStack } from './stacks/ApiStack';
import { AthenaStack } from './stacks/AthenaStack';

const app = new App();

new DataStack(app, "BulkEmailDataStack");

new AthenaStack(app, "BulkEmailAthenaStack")

const lambdaStack = new LambdaStack(app, "BulkEmailLambdaStack");

new StepFunctionsStack(app, "BulkEmailStepFunctionsStack", {
    processFileLambda: lambdaStack.processFileLambda,
    sendEmailLambda: lambdaStack.sendEmailLambda,
    notificationLambda: lambdaStack.notificationLambda,
});

new ApiStack(app, "BulkEmailApiStack", {
    startStepFunctionLambdaIntegration: lambdaStack.startStepFunctionLambdaIntegration,
});

