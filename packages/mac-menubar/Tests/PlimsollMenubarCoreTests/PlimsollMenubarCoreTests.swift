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
        #expect(invocation.arguments == ["--silent", "--dir", "/tmp/plimsoll checkout", "collector", "status"])
    }

    @Test func checkoutInvocationWithoutPnpmPathResolvesPnpmThroughEnv() throws {
        let invocation = try #require(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_REPO": "/tmp/plimsoll"])
        )

        #expect(invocation.executablePath == "/usr/bin/env")
        #expect(invocation.arguments == ["pnpm", "--silent", "--dir", "/tmp/plimsoll", "collector", "status"])
    }

    /// What `pnpm --dir <repo> collector status` printed without --silent.
    @Test func statusOutputThatIsNotJSONIsRejected() throws {
        let invocation = try #require(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_REPO": "/tmp/plimsoll"])
        )
        let banner = "\n> @plimsoll/monorepo@0.1.0 collector /tmp/plimsoll\n> tsx packages/collector-cli/src/cli.ts status\n\n"
        for stdout in [banner + #"{"port":48271,"stats":null}"#, "", "not json", "[]"] {
            let client = CollectorClient(
                invocation: invocation,
                execute: { _ in CollectorExecutionResult(standardOutput: stdout, standardError: "", exitCode: 0) },
                probeLiveness: { _ in true }
            )

            #expect(throws: CollectorClientError.invalidStatusOutput) { try client.snapshot() }
        }
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

    @Test func statusLargerThanThePipeBufferIsReadWithoutDeadlock() throws {
        // The collector's status JSON grows with the host; this one is ~1 MB.
        let collector = try FakeCollector("""
            printf '{"port":48271,"stats":{"count":3,"tokenAttributedEvents":3},"padding":"'
            head -c 1000000 /dev/zero | tr '\\0' 'x'
            printf '"}'
            """)
        defer { collector.remove() }

        let result = try ProcessCollectorExecutor.run(collector.invocation, timeout: 20)

        #expect(result.exitCode == 0)
        #expect(result.standardOutput.utf8.count > 1_000_000)
        #expect(try CollectorStatus(json: Data(result.standardOutput.utf8)).eventCount == 3)
    }

    @Test func hungCollectorTimesOutAndIsStopped() throws {
        // The fixture records its pid first; 2 s leaves room to start on a
        // loaded host before the deadline stops it.
        let collector = try FakeCollector("""
            echo $$ > "$0.pid"
            exec /bin/sleep 30
            """)
        defer { collector.remove() }

        #expect(throws: CollectorClientError.timedOut(seconds: 2)) {
            try ProcessCollectorExecutor.run(collector.invocation, timeout: 2)
        }
        let pidText = try String(contentsOf: collector.directory.appendingPathComponent("plimsoll.pid"), encoding: .utf8)
        let pid = try #require(pid_t(pidText.trimmingCharacters(in: .whitespacesAndNewlines)))
        #expect(kill(pid, 0) == -1 && errno == ESRCH, "the timed-out collector process is still running")
    }

    @Test func collectorFailureReportsExitCodeAndError() throws {
        let collector = try FakeCollector("""
            echo 'Error: database is locked' >&2
            exit 3
            """)
        defer { collector.remove() }

        #expect(throws: CollectorClientError.commandFailed(exitCode: 3, message: "Error: database is locked")) {
            try CollectorClient(invocation: collector.invocation, probeLiveness: { _ in false }).status()
        }
    }

    @Test func missingCollectorExecutableReportsLaunchFailure() throws {
        let invocation = try #require(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": "/nonexistent/plimsoll"])
        )

        #expect {
            try ProcessCollectorExecutor.run(invocation, timeout: 5)
        } throws: { error in
            guard case .processLaunchFailed = error as? CollectorClientError else { return false }
            return true
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

/// A throwaway executable standing in for `plimsoll`; it is run as
/// `<path> status`, exactly as the menubar runs the real collector.
private struct FakeCollector {
    let directory: URL
    let invocation: CollectorInvocation

    init(_ body: String) throws {
        directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("plimsoll-menubar-test-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("plimsoll")
        try ("#!/bin/sh\n" + body + "\n").write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        invocation = try #require(CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": executable.path]))
    }

    func remove() {
        try? FileManager.default.removeItem(at: directory)
    }
}
