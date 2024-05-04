import { GetObjectCommand, ListObjectsCommand, S3Client } from "@aws-sdk/client-s3";
import * as Minio from "minio";
// @ts-expect-error 
import * as cliProgress from "cli-progress";

const env = process.env;

export const srcClient = new S3Client({
    endpoint: 'https://fra1.digitaloceanspaces.com',
    forcePathStyle: false,
    region: 'us-east-1',
    credentials: {
        accessKeyId: env.S3_ACCESS_KEY_ID!,
        secretAccessKey: env.S3_SECRET_ACCESS_KEY!
    }
});

export const destClient = new Minio.Client({
    endPoint: "192.168.2.96",
    port: 9090,
    useSSL: false,
    accessKey: env.MINIO_ACCESS_KEY_ID!,
    secretKey: env.MINIO_SECRET_KEY!,
});

async function clone(key: string) {
    const get_params = {
        Bucket: env.S3_BUCKET,
        Key: key
    };

    const get_response = await srcClient.send(new GetObjectCommand(get_params));
    if (!get_response.Body) {
        return;
    }

    const size = get_response.ContentLength;
    if (!size || size < 100) {
        return;
    }

    // test if the object exists
    try {
        const exists = await destClient.statObject(env.MINIO_BUCKET!, key);
        if (exists) {
            return;
        }
    } catch (e) {
        // do nothing
    }

    const body = get_response.Body as any;
    await destClient.putObject(env.MINIO_BUCKET!, key, body, size, {
        "Content-Type": get_response.ContentType
    });
}

async function main() {
    const list_params = {
        Bucket: env.S3_BUCKET
    };

    const list_response = await srcClient.send(new ListObjectsCommand(list_params));
    if (!list_response.Contents) {
        return;
    }

    const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    bar.start(list_response.Contents.length, 0);

    for (const content of list_response.Contents!) {
        await clone(content.Key!);
        bar.increment();
    }
}

main();