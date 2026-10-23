import { AgentDoc, LiveDoc, configure, type Conversation } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import type { AgentInput } from "./input.ts";
import type { RequestId } from "./domain.ts";

/**
 * Configuration is a separate Pi commit, before submission. Admission is sequential:
 * after an uncertain commit the worker closes, and reopens before retrying.
 * A known Pi request NEVER reapplies configuration, even if its receipt mapping was lost.
 * A changed model waits for an active run; same-model steering keeps Pi's normal behavior.
 */
export async function admitInput(root: Conversation, input: AgentInput, request: RequestId) {
  const prepared = await root.commit(async (tx) => {
    const existing = await tx.submissionByRequest(root.id, request);
    if (existing) return { kind: "submitted", id: existing.id } as const;
    const agent = await tx.doc(AgentDoc, root.id);
    const model = input.model ?? agent.model;
    if (!model) {
      const rejected = await tx.createSubmission({
        conversationId: root.id,
        requestId: request,
        type: "input",
        status: "unanswered",
        reason: "model_required",
      });
      return { kind: "submitted", id: rejected.id } as const;
    }
    if (model.provider !== agent.model?.provider || model.modelId !== agent.model?.modelId) {
      // After failure Pi can be idle with queued inputs. The next submit drains them;
      // blocking on inbox contents would prevent that recovery indefinitely.
      if ((await tx.doc(LiveDoc, root.id)).run) {
        return { kind: "busy" } as const;
      }
      await configure(tx, root.id, { model });
    }
    return { kind: "ready" } as const;
  }, context);
  if (prepared.kind === "busy") return undefined;
  if (prepared.kind === "submitted") return { id: prepared.id };
  return root.submit(
    {
      type: "input",
      content: input.content,
      whenBusy: input.whenBusy,
      requestId: request,
    },
    context,
  );
}
