{
  description = "Development tools for pi-durable-aws-serverless";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/f20cd90e28fe0403873f473b29e42662cd86e8c4";

  outputs = { nixpkgs, ... }:
    let
      systems = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
    in {
      devShells = nixpkgs.lib.genAttrs systems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in {
          default = pkgs.mkShellNoCC {
            packages = [
              pkgs.nodejs_24
              pkgs.gitMinimal
              pkgs.pnpm_12
              # CLI + Node host only; the SDK selects its AWS plugin on approved use.
              (pkgs.pulumi.withPackages (ps: [ ps.pulumi-nodejs ]))
              pkgs.awscli2
            ];
          };
        });
    };
}
