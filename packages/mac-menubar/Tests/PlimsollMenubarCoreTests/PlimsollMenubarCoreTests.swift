import Foundation
import XCTest
@testable import PlimsollMenubarCore

final class PlimsollMenubarCoreTests: XCTestCase {
    func testPackagedInvocationUsesBinaryAndCommandWithoutShell() throws {
        let invocation = try XCTUnwrap(
            CollectorInvocation(environment: [
                "PLIMSOLL_COLLECTOR_BIN": "/tmp/plimsoll collector",
            ])
        )

        XCTAssertEqual(invocation.executablePath, "/tmp/plimsoll collector")
        XCTAssertEqual(invocation.arguments, ["status"])
    }

    func testCheckoutInvocationUsesFixedPnpmArguments() throws {
        let invocation = try XCTUnwrap(
            CollectorInvocation(environment: [
                "PLIMSOLL_COLLECTOR_REPO": "/tmp/plimsoll checkout",
                "PLIMSOLL_PNPM_BIN": "/tmp/pnpm",
            ], command: .stop)
        )

        XCTAssertEqual(invocation.executablePath, "/tmp/pnpm")
        XCTAssertEqual(
            invocation.arguments,
            ["--dir", "/tmp/plimsoll checkout", "collector", "stop"]
        )
    }

    func testStatusParsesCountsAndComputesTokenCoverage() throws {
        let status = try CollectorStatus(json: Data(#"{"port":48271,"stats":{"count":8,"tokenAttributedEvents":2,"totalInputTokens":100,"totalOutputTokens":50}}"#.utf8))

        XCTAssertEqual(status.port, 48271)
        XCTAssertEqual(status.eventCount, 8)
        XCTAssertEqual(status.tokenAttributedEvents, 2)
        XCTAssertEqual(status.totalInputTokens, 100)
        XCTAssertEqual(status.totalOutputTokens, 50)
        XCTAssertEqual(status.tokenCoveragePercent, 25)
    }

    func testStatusLeavesCoverageUnavailableWhenStatsAreMissingOrEmpty() throws {
        let missing = try CollectorStatus(json: Data(#"{"port":48271,"stats":null}"#.utf8))
        let empty = try CollectorStatus(json: Data(#"{"port":48271,"stats":{"count":0,"tokenAttributedEvents":0}}"#.utf8))

        XCTAssertNil(missing.eventCount)
        XCTAssertNil(missing.tokenCoveragePercent)
        XCTAssertNil(empty.tokenCoveragePercent)
    }

    func testClientSnapshotCombinesStatusWithLoopbackLiveness() throws {
        let invocation = try XCTUnwrap(
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

        XCTAssertTrue(snapshot.running)
        XCTAssertEqual(snapshot.port, 49123)
        XCTAssertEqual(snapshot.eventCount, 4)
        XCTAssertEqual(snapshot.tokenCoveragePercent, 25)
    }

    func testClientRejectsNonZeroCollectorExit() throws {
        let invocation = try XCTUnwrap(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": "/tmp/plimsoll"])
        )
        let client = CollectorClient(
            invocation: invocation,
            execute: { _ in
                CollectorExecutionResult(standardOutput: "", standardError: "failed", exitCode: 1)
            },
            probeLiveness: { _ in false }
        )

        XCTAssertThrowsError(try client.snapshot()) { error in
            XCTAssertEqual(error as? CollectorClientError, .commandFailed(exitCode: 1, message: "failed"))
        }
    }

    func testStartLaunchesForegroundDaemonWithoutWaitingForItToExit() throws {
        let base = try XCTUnwrap(
            CollectorInvocation(environment: ["PLIMSOLL_COLLECTOR_BIN": "/tmp/plimsoll"])
        )
        var launched: CollectorInvocation?
        let client = CollectorClient(
            invocation: base,
            execute: { _ in
                XCTFail("start must use the asynchronous launcher")
                return CollectorExecutionResult(standardOutput: "", standardError: "", exitCode: 0)
            },
            launch: { invocation in
                launched = invocation
            },
            probeLiveness: { _ in false }
        )

        XCTAssertNoThrow(try client.start())
        XCTAssertEqual(launched?.command, .start)
        XCTAssertEqual(launched?.arguments, ["start"])
    }

    func testPermissionDoctorReportsNoAdditionalPermissions() {
        let report = PermissionDoctor.report()

        XCTAssertFalse(report.accessibility)
        XCTAssertFalse(report.camera)
        XCTAssertFalse(report.inputMonitoring)
        XCTAssertFalse(report.microphone)
        XCTAssertFalse(report.screenRecording)
        XCTAssertFalse(report.requestsAdditionalPermissions)
        XCTAssertEqual(report.summary, "No additional macOS permissions requested")

        XCTAssertNoThrow {
            let json = try PermissionDoctor.json()
            XCTAssertTrue(json.contains("No additional macOS permissions requested"))
            XCTAssertTrue(json.contains("\"requestsAdditionalPermissions\":false"))
        }
    }
}
