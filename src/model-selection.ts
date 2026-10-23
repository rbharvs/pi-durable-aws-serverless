import { OPENAI_MODELS } from "@earendil-works/pi-ai/providers/openai.models";
import { ANTHROPIC_MODELS } from "@earendil-works/pi-ai/providers/anthropic.models";

/** Chat catalogs only: API validation must not import provider clients or execution code. */
const catalogs = { openai: OPENAI_MODELS, anthropic: ANTHROPIC_MODELS };

/** Current admission policy only; never use to validate a historical receipt on read. */
export function isSupportedModel(provider: string, modelId: string): boolean {
  return (
    Object.hasOwn(catalogs, provider) &&
    Object.values(catalogs[provider as keyof typeof catalogs]).some((model) => model.id === modelId)
  );
}
