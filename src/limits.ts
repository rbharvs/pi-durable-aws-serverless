/** Conservative physical publication limits, including SQL and scalar bind overhead. */
export const LIMITS = Object.freeze({
  maxRows: 1000,
  maxLogicalCommitBytes: 32 * 1024 * 1024,
  maxTransactionBytes: 8 * 1024 * 1024,
  maxInlineBytes: 256 * 1024,
  maxObjectBytes: 16 * 1024 * 1024,
});
