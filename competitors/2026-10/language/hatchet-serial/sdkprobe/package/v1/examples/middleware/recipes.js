"use strict";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment -- Demo file with external deps not in SDK
// @ts-nocheck
// These snippets demonstrate common middleware patterns.
// They reference external packages (@aws-sdk/*) that are NOT
// dependencies of the Hatchet SDK — install them in your own project.
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
// > End-to-end encryption
const v1_1 = require("../..");
const crypto_1 = require("crypto");
// !!
// > Offloading large payloads to S3
const client_s3_1 = require("@aws-sdk/client-s3");
const s3_request_presigner_1 = require("@aws-sdk/s3-request-presigner");
const ALGORITHM = 'aes-256-gcm';
const KEY = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
function encrypt(plaintext) {
    const iv = (0, crypto_1.randomBytes)(16);
    const cipher = (0, crypto_1.createCipheriv)(ALGORITHM, KEY, iv);
    const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
    return {
        ciphertext: encrypted.toString('base64'),
        iv: iv.toString('base64'),
        tag: cipher.getAuthTag().toString('base64'),
    };
}
function decrypt(ciphertext, iv, tag) {
    const decipher = (0, crypto_1.createDecipheriv)(ALGORITHM, KEY, Buffer.from(iv, 'base64'));
    decipher.setAuthTag(Buffer.from(tag, 'base64'));
    return decipher.update(ciphertext, 'base64', 'utf8') + decipher.final('utf8');
}
const e2eEncryption = {
    before: (input) => {
        if (!input.encrypted) {
            return input;
        }
        const { ciphertext, iv, tag } = input.encrypted;
        const decrypted = JSON.parse(decrypt(ciphertext, iv, tag));
        return Object.assign(Object.assign(Object.assign({}, input), decrypted), { encrypted: undefined });
    },
    after: (output) => {
        const payload = JSON.stringify(output);
        return { encrypted: encrypt(payload) };
    },
};
const encryptionClient = v1_1.HatchetClient.init().withMiddleware(e2eEncryption);
const s3 = new client_s3_1.S3Client({ region: process.env.AWS_REGION });
const BUCKET = process.env.S3_BUCKET;
const PAYLOAD_THRESHOLD = 256 * 1024; // 256 KB
function uploadToS3(data) {
    return __awaiter(this, void 0, void 0, function* () {
        const key = `hatchet-payloads/${(0, crypto_1.randomUUID)()}.json`;
        yield s3.send(new client_s3_1.PutObjectCommand({
            Bucket: BUCKET,
            Key: key,
            Body: JSON.stringify(data),
            ContentType: 'application/json',
        }));
        return (0, s3_request_presigner_1.getSignedUrl)(s3, new client_s3_1.GetObjectCommand({ Bucket: BUCKET, Key: key }), {
            expiresIn: 3600,
        });
    });
}
function downloadFromS3(url) {
    return __awaiter(this, void 0, void 0, function* () {
        const res = yield fetch(url);
        return res.json();
    });
}
const s3Offload = {
    before: (input) => __awaiter(void 0, void 0, void 0, function* () {
        if (input.s3Url) {
            const restored = (yield downloadFromS3(input.s3Url));
            return Object.assign(Object.assign({}, restored), { s3Url: undefined });
        }
        return input;
    }),
    after: (output) => __awaiter(void 0, void 0, void 0, function* () {
        const serialized = JSON.stringify(output);
        if (serialized.length > PAYLOAD_THRESHOLD) {
            const url = yield uploadToS3(output);
            return { s3Url: url };
        }
        return output;
    }),
};
const s3Client = v1_1.HatchetClient.init().withMiddleware(s3Offload);
// !!
