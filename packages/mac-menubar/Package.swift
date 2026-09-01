// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "PlimsollMenubar",
    platforms: [
        .macOS(.v13),
    ],
    products: [
        .library(
            name: "PlimsollMenubarCore",
            targets: ["PlimsollMenubarCore"]
        ),
        .executable(
            name: "plimsoll-menubar",
            targets: ["PlimsollMenubar"]
        ),
    ],
    targets: [
        .target(name: "PlimsollMenubarCore"),
        .executableTarget(
            name: "PlimsollMenubar",
            dependencies: ["PlimsollMenubarCore"]
        ),
        .testTarget(
            name: "PlimsollMenubarCoreTests",
            dependencies: ["PlimsollMenubarCore"]
        ),
    ]
)
