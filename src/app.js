// app.js (Node.js 20.x, AWS SDK v3 - CommonJS)

const { TextractClient, DetectDocumentTextCommand } = require("@aws-sdk/client-textract");
const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const { randomUUID } = require("crypto");

const textract = new TextractClient({});
const dynamodb = new DynamoDBClient({});
const TABLE_NAME = process.env.TABLE_NAME;

// -------------- Helper functions for parsing text -----------------

function extractEmail(text) {
  const match = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
  return match ? match[0] : null;
}

function extractPhone(text) {
  // Very simple phone regex; tune for your formats
  const match = text.match(/(\+?\d[\d\s\-]{8,15})/);
  return match ? match[0] : null;
}

function extractName(text) {
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  for (const line of lines.slice(0, 8)) {
    const lower = line.toLowerCase();
    if (lower.includes("resume") || lower.includes("curriculum") || lower.includes("vitae")) continue;
    if (line.includes("@")) continue;
    if (/\d/.test(line)) continue; // skip if contains numbers
    if (line.split(/\s+/).length <= 5) {
      return line;
    }
  }
  return null;
}

function extractSkills(text) {
  const knownSkills = [
    "Java",
    "Spring Boot",
    "Python",
    "Node.js",
    "JavaScript",
    "TypeScript",
    "React",
    "Angular",
    "AWS",
    "Docker",
    "Kubernetes",
    "Spark",
    "Hadoop",
    "SQL",
    "NoSQL",
    "Kafka"
  ];

  const found = new Set();
  const lower = text.toLowerCase();

  for (const skill of knownSkills) {
    if (lower.includes(skill.toLowerCase())) {
      found.add(skill);
    }
  }

  return Array.from(found).sort();
}

// -------------- Textract integration -----------------

async function extractTextFromS3(bucket, key) {
  const cmd = new DetectDocumentTextCommand({
    Document: {
      S3Object: {
        Bucket: bucket,
        Name: key
      }
    }
  });

  const response = await textract.send(cmd);

  const lines = [];
  if (response.Blocks) {
    for (const block of response.Blocks) {
      if (block.BlockType === "LINE" && block.Text) {
        lines.push(block.Text);
      }
    }
  }
  return lines.join("\n");
}

// -------------- DynamoDB integration -----------------

async function saveCandidateToDynamo(item) {
  const params = {
    TableName: TABLE_NAME,
    Item: {
      candidateId: { S: item.candidateId },
      bucket: { S: item.bucket },
      fileKey: { S: item.fileKey },
      rawText: { S: item.rawText }
    }
  };

  if (item.name) params.Item.name = { S: item.name };
  if (item.email) params.Item.email = { S: item.email };
  if (item.phone) params.Item.phone = { S: item.phone };
  if (item.skills && item.skills.length > 0) {
    params.Item.skills = { SS: item.skills };
  }

  const cmd = new PutItemCommand(params);
  await dynamodb.send(cmd);
}

// -------------- Lambda handler -----------------

exports.handler = async (event, context) => {
  console.log("Received event:", JSON.stringify(event, null, 2));

  if (!event.Records || event.Records.length === 0) {
    console.log("No records in event");
    return { statusCode: 200, body: "No records" };
  }

  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    console.log(`Processing file: s3://${bucket}/${key}`);

    try {
      // 1. Get text from Textract
      const text = await extractTextFromS3(bucket, key);
      console.log("Extracted text length:", text.length);

      // 2. Parse info
      const email = extractEmail(text);
      const phone = extractPhone(text);
      const name = extractName(text);
      const skills = extractSkills(text);

      const candidateId = randomUUID();

      const item = {
        candidateId,
        bucket,
        fileKey: key,
        rawText: text,
        name,
        email,
        phone,
        skills
      };

      console.log("Parsed candidate item:", JSON.stringify(item, null, 2));

      // 3. Save to DynamoDB
      await saveCandidateToDynamo(item);
      console.log(`Saved candidate ${candidateId} to DynamoDB`);
    } catch (err) {
      console.error(`Error processing file s3://${bucket}/${key}`, err);
      // Continue to next record
    }
  }

  return {
    statusCode: 200,
    body: "OK"
  };
};