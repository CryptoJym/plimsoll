import Foundation
import Testing
@testable import PlimsollMenubarCore

struct PlimsollMenubarCoreTests {
    @Test func packagedInvocationUsesBinaryAndCommandWithoutShell() throws {
        let invocation = try #require(
            CollectorInvocation(environment: [
                "PLIMSOLL_COLLECTOR_BIN": "/tmp/plimsoll collector",
            ])
        )

        #expect(invocation.executablePath == "/tmp/plimsoll collector")
        #expect(invocation.arguments == ["status"])
    }

    @Test func checkoutInvocationUsesFixedPnpmArguments() throws {
        let invocation = try #require(
            CollectorInvocation(environment: [
                "PLIMSOLL_COLLECTOR_REPO": "/tmp/plimsoll checkout",
                "PLIMSOLL_PNPM_BIN": "/tmp/pnpm",
            ])
        )

        #expect(invocation.executablePath == "/tmp/pnpm")
        #expect(invocation.arguments == ["--dir", "/tmp/plimsoll checkout", "collector", "status"])
    }

    @Test func checkoutInvocationWithoutPnpmPathResolvesPnpmThroughEnv() throws {
        let invocation = try #require(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_REPO": "/tmp/plimsoll"])
        )

        #expect(invocation.executablePath == "/usr/bin/env")
        #expect(invocation.arguments == ["pnpm", "--dir", "/tmp/plimsoll", "collector", "status"])
    }

    @Test func noConfiguredCollectorBuildsNoInvocation() {
        #expect(CollectorInvocation(environment: [:]) == nil)
        #expect(CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": "  "]) == nil)
    }

    /// The menubar is read-only: whatever it is pointed at, it runs `status`.
    @Test(arguments: [
        ["PLIMSOLL_COLLECTOR_BIN": "/opt/plimsoll/bin/plimsoll"],
        ["PLIMSOLL_COLLECTOR_REPO": "/src/plimsoll"],
        ["PLIMSOLL_COLLECTOR_REPO": "/src/plimsoll", "PLIMSOLL_PNPM_BIN": "/opt/pnpm"],
    ])
    func everyInvocationRunsOnlyTheStatusCommand(environment: [String: String]) throws {
        let invocation = try #require(CollectorInvocation(environment: environment))

        #expect(invocation.arguments.last == "status")
        #expect(invocation.arguments.filter { ["start", "stop", "restart", "setup"].contains($0) }.isEmpty)
    }

    @Test func statusParsesCountsAndComputesTokenCoverage() throws {
        let status = try CollectorStatus(json: Data(#"{"port":48271,"stats":{"count":8,"tokenAttributedEvents":2,"totalInputTokens":100,"totalOutputTokens":50}}"#.utf8))

        #expect(status.port == 48271)
        #expect(status.eventCount == 8)
        #expect(status.tokenAttributedEvents == 2)
        #expect(status.totalInputTokens == 100)
        #expect(status.totalOutputTokens == 50)
        #expect(status.tokenCoveragePercent == 25)
    }

    @Test func statusLeavesCoverageUnavailableWhenStatsAreMissingOrEmpty() throws {
        let missing = try CollectorStatus(json: Data(#"{"port":48271,"stats":null}"#.utf8))
        let empty = try CollectorStatus(json: Data(#"{"port":48271,"stats":{"count":0,"tokenAttributedEvents":0}}"#.utf8))

        #expect(missing.eventCount == nil)
        #expect(missing.tokenCoveragePercent == nil)
        #expect(empty.tokenCoveragePercent == nil)
    }

    @Test func clientSnapshotCombinesStatusWithLoopbackLiveness() throws {
        let invocation = try #require(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": "/tmp/plimsoll"])
        )
        let client = CollectorClient(
            invocation: invocation,
            execute: { _ in
                CollectorExecutionResult(
                    standardOutput: #"{"port":49123,"stats":{"count":4,"tokenAttributedEvents":1}}"#,
                    standardError: "",
                    exitCode: 0
                )
            },
            probeLiveness: { port in port == 49123 }
        )

        let snapshot = try client.snapshot()

        #expect(snapshot.running)
        #expect(snapshot.port == 49123)
        #expect(snapshot.eventCount == 4)
        #expect(snapshot.tokenCoveragePercent == 25)
    }

    @Test func clientRejectsNonZeroCollectorExit() throws {
        let invocation = try #require(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": "/tmp/plimsoll"])
        )
        let client = CollectorClient(
            invocation: invocation,
            execute: { _ in
                CollectorExecutionResult(standardOutput: "", standardError: "failed", exitCode: 1)
            },
            probeLiveness: { _ in false }
        )

        #expect(throws: CollectorClientError.commandFailed(exitCode: 1, message: "failed")) {
            try client.snapshot()
        }
    }

    @Test func permissionDoctorReportsNoAdditionalPermissions() throws {
        let report = PermissionDoctor.report()

        #expect(!report.accessibility)
        #expect(!report.camera)
        #expect(!report.inputMonitoring)
        #expect(!report.microphone)
        #expect(!report.screenRecording)
        #expect(!report.requestsAdditionalPermissions)
        #expect(report.summary == "No additional macOS permissions requested")

        // The XCTest version wrapped these in XCTAssertNoThrow { ... }, which
        // returns the closure without calling it; they now actually run.
        let json = try PermissionDoctor.json()
        #expect(json.contains("No additional macOS permissions requested"))
        #expect(json.contains("\"requestsAdditionalPermissions\":false"))
    }
}
