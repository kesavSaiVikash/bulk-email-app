import { CfnOutput, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { LambdaInvoke } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { StateMachine, DefinitionBody, LogLevel } from 'aws-cdk-lib/aws-stepfunctions';
import { Construct } from 'constructs';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { LogGroup } from 'aws-cdk-lib/aws-logs';

interface StepFunctionsStackProps extends StackProps {
    processFileLambda: NodejsFunction;
    sendEmailLambda: NodejsFunction;
    notificationLambda: NodejsFunction;
}

export class StepFunctionsStack extends Stack {
    public readonly stateMachineArn: string;

    constructor(scope: Construct, id: string, props: StepFunctionsStackProps) {
        super(scope, id, props);

        const processFileTask = new LambdaInvoke(this, 'Process CSV File', {
            lambdaFunction: props.processFileLambda,
            resultPath: '$.processFileResult',
        });

        const sendEmailTask = new LambdaInvoke(this, 'Send Emails', {
            lambdaFunction: props.sendEmailLambda,
            inputPath: '$.processFileResult',
            resultPath: '$.sendEmailResult',
        });

        const notifyTask = new LambdaInvoke(this, 'Notify Results', {
            lambdaFunction: props.notificationLambda,
            inputPath: '$.sendEmailResult',
        });

        const definition = DefinitionBody.fromChainable(
            processFileTask.next(sendEmailTask).next(notifyTask)
        )

        const stepFunction = new StateMachine(this, 'BulkEmailStateMachine', {
            definitionBody: definition,
            logs: {
                destination: new LogGroup(this, 'StepFunctionLogs', { removalPolicy: RemovalPolicy.DESTROY }),
                level: LogLevel.ALL,
            },
        });

        this.stateMachineArn = stepFunction.stateMachineArn

        // Store the State Machine ARN in SSM Parameter Store
        new StringParameter(this, 'StateMachineArnParameter', {
            parameterName: '/bulk-email/state-machine-arn',
            stringValue: this.stateMachineArn,
        });

        new CfnOutput(this, 'StepFunctionArn', {
            value: stepFunction.stateMachineArn,
            exportName: 'StepFunctionArn',
        });
    }
}
