/** A real operator entry used only to test packaging. No live provider or service invocation. */
import { createModels, fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { createRegistry } from "@earendil-works/pi-durable";
import { createAwsHandler } from "../../src/aws/worker.ts";

const models = createModels();
models.setProvider(fauxProvider({ models: [{ id: "offline", contextWindow: 8000000 }] }).provider);
models.streamSimple = () => {
  const stream = new AssistantMessageEventStream();
  stream.push({ type: "done", reason: "stop", message: fauxAssistantMessage("offline output") });
  stream.end();
  return stream;
};
export const handler = createAwsHandler(
  { models, registry: createRegistry() },
  { model: { provider: "faux", modelId: "offline" } },
);
