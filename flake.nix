# https://fasterthanli.me/series/building-a-rust-service-with-nix/part-10
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in
      with pkgs; {
        devShells.default = mkShell.override { stdenv = clang15Stdenv; } {
          nativeBuildInputs = [
            boost
            cmake
            jemalloc
            glog
            zlib
          ];

          buildInputs = [
            (python3.withPackages (python-pkgs: with python-pkgs; [
              matplotlib
              pandas
              plotly
              python-lsp-ruff
              python-lsp-server
            ]))
          ];
        };
      }
    );
}
