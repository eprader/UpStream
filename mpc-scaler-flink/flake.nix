{
  description = "MPC scaler for the Flink Kubernetes cluster based on a Kalman filter";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
  };

  outputs =
    { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

    in
    {
      devShells.${system} = {
        default = pkgs.mkShell {
          buildInputs = with pkgs; [
            poetry

            kubectl
            k3d
            kubernetes-helm
          ];

          env = {
            # NOTE: Adding `(nix-shell)` as a visual marker when a Nix dev shell is active.
            PS1 = "(nix-shell) $PS1";
            # INFO:
            # Some python packages depend on dynamic libraries to be available via the linker.
            # In Nix, these libraries do not live in the expected directories.
            # Therefore, if one wants to make these libraries available one needs to add them manually.
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.gcc14.cc.lib
              pkgs.libz
            ];
          };

          shellHook = ''
            export KUBECONFIG=$PWD/../.kube/config
          '';
        };
      };
    };
}
