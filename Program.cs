using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using System;
using System.Threading.Tasks;

namespace S3CopyObjectCrossAccount
{
    class Program
    {
        private static readonly RegionEndpoint sourceBucketRegion = RegionEndpoint.USEast1;
        private static readonly RegionEndpoint destinationBucketRegion = RegionEndpoint.USWest2;
        private static IAmazonS3 s3Client;

        private const string sourceBucket = "source-bucket-name";
        private const string destinationBucket = "destination-bucket-name";
        private const string objectKey = "object-key";

        private const string roleArn = "arn:aws:iam::destination-account-id:role/role-name";

        static void Main()
        {
            s3Client = new AmazonS3Client(sourceBucketRegion);
            Console.WriteLine("Copying an object");
            CopyingObjectAsync().Wait();
        }

        private static async Task CopyingObjectAsync()
        {
            try
            {
                AmazonSecurityTokenServiceClient stsClient = new AmazonSecurityTokenServiceClient();
                AssumeRoleRequest assumeRequest = new AssumeRoleRequest
                {
                    RoleArn = roleArn,
                    RoleSessionName = "S3CopySession"
                };
                AssumeRoleResponse assumeResponse = await stsClient.AssumeRoleAsync(assumeRequest);

                AmazonS3Client s3ClientWithRole = new AmazonS3Client(
                    assumeResponse.Credentials,
                    destinationBucketRegion
                );

                CopyObjectRequest request = new CopyObjectRequest
                {
                    SourceBucket = sourceBucket,
                    SourceKey = objectKey,
                    DestinationBucket = destinationBucket,
                    DestinationKey = objectKey
                };
                CopyObjectResponse response = await s3ClientWithRole.CopyObjectAsync(request);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine(
                    "Error encountered on server. Message:'{0}' when writing an object",
                    e.Message
                );
            }
            catch (Exception e)
            {
                Console.WriteLine(
                    "Unknown encountered on server. Message:'{0}' when writing an object",
                    e.Message
                );
            }
        }
    }
}
