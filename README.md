# Pi Durable on AWS Serverless

This repository will contain a working example of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) running on AWS Lambda and Amazon Aurora DSQL.

## Development

Requires Nix with flakes enabled.

Install dependencies:

```sh
nix develop --command pnpm install --frozen-lockfile
```

Check formatting:

```sh
nix develop --command pnpm check
```

Format files:

```sh
nix develop --command pnpm format
```

## AI usage

AI coding agents contributed substantially to research, design, implementation, testing, and review. The maintainer set the requirements and made the final product and architectural decisions.

## License

[MIT](LICENSE)
