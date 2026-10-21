/** Impossible file paths: /dev/null is a file on the supported Nix/macOS/Linux hosts. */
export const awsTestEnv = {
  AWS_CONFIG_FILE: "/dev/null/pi-durable-test-config",
  AWS_SHARED_CREDENTIALS_FILE: "/dev/null/pi-durable-test-credentials",
  AWS_EC2_METADATA_DISABLED: "true",
  AWS_IGNORE_CONFIGURED_ENDPOINT_URLS: "true",
};
