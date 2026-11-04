{ pkgs ? import <nixpkgs> { } }:
with pkgs;
let
  hpkgs = haskellPackages.override {
    overrides = self: super: {
      # Newer than nixpkgs has.
      net-mqtt = haskell.lib.dontCheck (self.callHackageDirect {
        pkg = "net-mqtt";
        ver = "0.8.7.1";
        sha256 = "0rqbw6q793nfxds25ng50b2dn11vj4niwhyc6gk4iw1q2k7rf7if";
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
