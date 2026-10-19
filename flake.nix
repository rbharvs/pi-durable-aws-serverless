{
  description = "Development tools for pi-durable-aws-serverless";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/3ed67ec0a4d3c7ab4ae1f04f8ee8df07bfa506a2";

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
              pkgs.nodejs_22
              pkgs.gitMinimal
              (pkgs.pnpm_10.override { nodejs-slim = pkgs.nodejs_22; })
            ];
          };
        });
    };
}
