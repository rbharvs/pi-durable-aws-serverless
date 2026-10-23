import { createRegistry } from "@earendil-works/pi-durable";
import { createProviderModels } from "../providers.ts";
import { createAwsHandler } from "./worker.ts";

// No model default: the first receipt selects it; Pi persists it for continuation.
export const handler = createAwsHandler(
  {
    models: createProviderModels(),
    registry: createRegistry(),
  },
  {},
);
