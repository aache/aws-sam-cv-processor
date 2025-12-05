// roleMatch.js - Node.js 20.x, AWS SDK v3, Amazon Titan Text Lite role matching

const { DynamoDBClient, UpdateItemCommand } = require("@aws-sdk/client-dynamodb");
const { BedrockRuntimeClient, InvokeModelCommand } = require("@aws-sdk/client-bedrock-runtime");
const { TextDecoder } = require("util");

const dynamodb = new DynamoDBClient({});
const bedrock = new BedrockRuntimeClient({
  // Use the region where Bedrock is enabled (Mumbai for you)
  region: process.env.AWS_REGION || "ap-south-1"
});

const TABLE_NAME = process.env.TABLE_NAME;
const ROLE_DESCRIPTION = process.env.ROLE_DESCRIPTION || "Generic Software Engineer";

const textDecoder = new TextDecoder();

// ------------------ Bedrock (Amazon Titan Text Lite) ------------------

async function evaluateCandidateWithBedrock(resumeText, roleDescription) {
  const prompt = `
You are an expert technical recruiter.

Analyze the following candidate resume text and determine how well they fit the given job role.

ROLE DESCRIPTION:
${roleDescription}

RESUME TEXT:
${resumeText}

You MUST respond with ONLY a single line of valid JSON.
Do not add any extra text, explanation, or markdown.

The JSON schema is:

{
  "fitScore": number between 0 and 100,
  "summary": string,
  "keyStrengths": [string],
  "concerns": [string],
  "skillsMatched": [string],
  "skillsMissing": [string],
  "recommendedLevel": "Junior | Mid | Senior | Lead | Principal"
}
`;

  // Titan text models use inputText + textGenerationConfig
  const body = {
    inputText: prompt,
    textGenerationConfig: {
      maxTokenCount: 512,
      temperature: 0.2,
      topP: 0.9,
      stopSequences: [] // you can tune this later
    }
  };

  const cmd = new InvokeModelCommand({
    modelId: "amazon.titan-text-lite-v1",
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify(body)
  });

  const response = await bedrock.send(cmd);
  const responseJson = JSON.parse(textDecoder.decode(response.body));
  console.log("Bedrock response JSON:", JSON.stringify(responseJson, null, 2));

  // Titan response: { results: [ { outputText: "..." , ... } ] }
  let textOut = responseJson?.results?.[0]?.outputText ?? "{}";
  textOut = textOut.trim();

  // Strip accidental code fences if Titan ever adds them
  if (textOut.startsWith("```")) {
    textOut = textOut
      .replace(/```json?/gi, "")
      .replace(/```/g, "")
      .trim();
  }

  try {
    // Happy path: Titan obeyed and returned JSON
    return JSON.parse(textOut);
  } catch (e) {
    // Fallback: Titan returned plain text; use it as summary instead of crashing
    console.error("Failed to parse Titan JSON, raw text:", textOut);

    return {
      fitScore: null,
      summary: textOut,
      keyStrengths: [],
      concerns: [],
      skillsMatched: [],
      skillsMissing: [],
      recommendedLevel: "Unknown"
    };
  }
}

// ------------------ DynamoDB update ------------------

async function updateCandidateAiFit(candidateId, aiFit) {
  const cmd = new UpdateItemCommand({
    TableName: TABLE_NAME,
    Key: {
      candidateId: { S: candidateId }
    },
    UpdateExpression: "SET aiFit = :fit",
    ExpressionAttributeValues: {
      ":fit": { S: JSON.stringify(aiFit) }
    }
  });

  await dynamodb.send(cmd);
}

// ------------------ Lambda handler (DynamoDB stream) ------------------

exports.handler = async (event) => {
  console.log("RoleMatchFunction event:", JSON.stringify(event, null, 2));

  if (!event.Records || event.Records.length === 0) {
    return;
  }

  for (const record of event.Records) {
    try {
      if (record.eventName !== "INSERT") {
        continue; // only handle new items
      }

      const newImage = record.dynamodb.NewImage || {};
      const candidateId = newImage.candidateId?.S;
      const rawText = newImage.rawText?.S;

      if (!candidateId || !rawText) {
        console.log("Missing candidateId or rawText, skipping record");
        continue;
      }

      // Avoid re-processing if aiFit already present
      if (newImage.aiFit) {
        console.log(`Candidate ${candidateId} already has aiFit, skipping`);
        continue;
      }

      console.log(`Evaluating candidateId=${candidateId}`);

      const aiFit = await evaluateCandidateWithBedrock(rawText, ROLE_DESCRIPTION);

      console.log("aiFit result:", JSON.stringify(aiFit, null, 2));

      await updateCandidateAiFit(candidateId, aiFit);

      console.log(`Updated candidate ${candidateId} with aiFit`);
    } catch (err) {
      console.error("Error processing record", JSON.stringify(record, null, 2), err);
    }
  }
};