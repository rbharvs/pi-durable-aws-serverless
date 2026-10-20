import { contracts } from "../tests/shared/storage.contract.ts";
import { memoryFixture } from "../tests/shared/memory.ts";
import { boundaries } from "../tests/shared/backend-boundaries.ts";
contracts("memory/logical Storage", () => memoryFixture(true));
boundaries("memory", () => memoryFixture(true));
