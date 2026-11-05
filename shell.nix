{ pkgs ? import <nixpkgs> { } }:
with pkgs;
let
  hpkgs = haskellPackages.override {
    overrides = self: super: {
      # Newer than nixpkgs has.
      net-mqtt = haskell.lib.dontCheck (self.callHackageDirect {
        pkg = "net-mqtt";
        ver = "0.8.8.0";
        sha256 = "1xg3yhzis53zw9bid9kf028f19wpbb98rz6xgwbz10j3pcwjm0yi";
      } { });
    };
  };
  ghcWithDeps = hpkgs.ghcWithPackages (p: with p; [
    async
    bytestring
    containers
    ekg
    ekg-core
    megaparsec
    monad-logger
    mtl
    net-mqtt
    network-uri
    optparse-applicative
    stm
    text
    transformers
    unliftio
    unliftio-core
    unordered-containers
    utf8-string
    # test deps
    QuickCheck
    attoparsec
    conduit
    tasty
    tasty-hunit
    tasty-quickcheck
  ]);
in
mkShell {
  buildInputs = [
    ghcWithDeps
    stylish-haskell
    hlint
    ghcid
    hpkgs.haskell-language-server
    cabal-install
    hpack
    zlib
  ];

  shellHook = ''
    # ...
  '';
}
