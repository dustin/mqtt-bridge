{ pkgs ? import <nixpkgs> { } }:
with pkgs;
let
  hpkgs = haskellPackages;
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
