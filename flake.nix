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
        devShells.default = mkShell.override { stdenv = llvmPackages_15.stdenv; } {
          nativeBuildInputs = [
            cmake
            jemalloc
            llvmPackages_15.bintools
          ];

          # https://discourse.nixos.org/t/libclang-path-and-rust-bindgen-in-nixpkgs-unstable/13264
          LIBCLANG_PATH = "${llvmPackages_latest.libclang.lib}/lib";
        };
      }
    );
}
