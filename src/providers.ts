import { createModels } from "@earendil-works/pi-ai/models";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";

/** Official Pi implementations; credentials are resolved lazily from worker environment only. */
export function createProviderModels() {
  const models = createModels({
    authContext: {
      env: async (name) =>
        name === "OPENAI_API_KEY" || name === "ANTHROPIC_API_KEY" ? process.env[name] : undefined,
      fileExists: async () => false,
    },
  });
  models.setProvider(openaiProvider());
  models.setProvider(anthropicProvider());
  return models;
}
