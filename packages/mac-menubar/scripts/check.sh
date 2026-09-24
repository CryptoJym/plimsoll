#!/bin/sh
# Build and test the menubar package: `swift build`, then `swift test`.
#
# The tests use Swift Testing (Swift 6 toolchains; the Command Line Tools ship
# no XCTest). With only the Command Line Tools installed, SwiftPM does not add
# the Swift Testing framework and interop library paths, so pass them here.
# Xcode 16+ needs no extra flags.
set -eu
cd "$(dirname "$0")/.."

swift build

clt=/Library/Developer/CommandLineTools
case "$(xcrun --find swift)" in
  "$clt"/*)
    swift test \
      -Xswiftc -F -Xswiftc "$clt/Library/Developer/Frameworks" \
      -Xlinker -rpath -Xlinker "$clt/Library/Developer/Frameworks" \
      -Xlinker -rpath -Xlinker "$clt/Library/Developer/usr/lib"
    ;;
  *)
    swift test
    ;;
esac
