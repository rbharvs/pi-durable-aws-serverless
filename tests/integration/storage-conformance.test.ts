import { registerConformance } from "../shared/conformance.ts";
import { documentFixture } from "./document-support.ts";
registerConformance("PostgreSQL", () => documentFixture("conformance"));
