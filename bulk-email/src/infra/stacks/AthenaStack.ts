import { CfnOutput, Fn, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { CfnDatabase, CfnTable } from 'aws-cdk-lib/aws-glue';

export class AthenaStack extends Stack {
    public readonly athenaDatabase: CfnDatabase;
    public readonly athenaTable: CfnTable;

    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const bucketName = Fn.importValue('BulkEmailS3BucketName');

        this.athenaDatabase = new CfnDatabase(this, 'AthenaDatabase', {
            catalogId: this.account,
            databaseInput: {
                name: 'bulk_email_s3_db',
            },
        });

        this.athenaTable = new CfnTable(this, 'AthenaTable', {
            databaseName: this.athenaDatabase.ref,
            catalogId: this.account,
            tableInput: {
                name: 'bulk-email-records-table',
                tableType: 'EXTERNAL_TABLE',
                parameters: {
                    'classification': 'csv',
                    'skip.header.line.count': '1',  // Skip the first row for header
                    'typeOfData': 'csv',

                },
                storageDescriptor: {
                    columns: [
                        {
                            name: 'email',
                            type: 'string',
                        },
                        {
                            name: 'firstName',
                            type: 'string',
                        },
                    ],
                    location: `s3://${bucketName}/bulk-email-data/`,
                    inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serdeInfo: {
                        name: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        parameters: {
                            'separatorChar': ',',
                            'quoteChar': '"',
                            'escapeChar': '\\'
                        },
                        serializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                    }
                },
            },
        });

        new CfnOutput(this, 'BulkEmailAthenaDbNameOutput', {
            value: this.athenaDatabase.ref,
            exportName: 'BulkEmailAthenaDbName',
        });

        new CfnOutput(this, 'BulkEmailAthenaTableNameOutput', {
            value: this.athenaTable.ref,
            exportName: 'BulkEmailAthenaTableName',
        });
    }
}
