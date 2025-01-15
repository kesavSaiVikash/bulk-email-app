// import { Readable } from 'stream';

// export function parseCSV(data: string): any[] {
//     // A simple CSV parser (consider using a library like 'csv-parse' for complex cases)
//     const rows = data.split("\n");

//     return rows.map((row) => {

//         const columns = row.split(",");

//         return {
//             email: columns[0],
//             firstName: columns[1],
//         };
//     });
// }

// export async function streamToBuffer(stream: Readable): Promise<Buffer> {
//     const chunks: Uint8Array[] = [];

//     for await (const chunk of stream) {
//         chunks.push(Buffer.from(chunk));
//     }

//     return Buffer.concat(chunks);
// }


// async function verifyDomain(domain: string) {
//     try {
//         const listIdentitiesCommand = new ListIdentitiesCommand({ IdentityType: "Domain" });

//         const verifiedIdentities = await sesClient.send(listIdentitiesCommand);

//         if (!verifiedIdentities.Identities?.includes(domain)) {
//             console.log(`Domain ${domain} is not verified, initiating verification...`);

//             const verifyDomainCommand = new VerifyDomainIdentityCommand({ Domain: domain });

//             const { VerificationToken } = await sesClient.send(verifyDomainCommand);

//             console.log("VerificationToken: " + VerificationToken)

//             return VerificationToken

//             console.log(`Domain verification request sent for ${domain}`);
//         } else {
//             console.log(`Domain ${domain} is already verified.`);
//         }
//     } catch (error) {
//         console.error("Error verifying domain:", error);
//     }
// }


// Helper function for exponential backoff with jitter
export const backoff = (attempt: number) => {
    const maxDelay = 60000; // maximum 1 minute delay
    const baseDelay = Math.pow(2, attempt) * 1000; // Exponential backoff, increasing delay each attempt
    const jitter = Math.random() * 1000; // Adding random jitter between 0 and 1 second
    return Math.min(baseDelay + jitter, maxDelay); // Apply the max delay cap
};
