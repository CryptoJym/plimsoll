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
        let collector = try FakeCollector(megabyteStatusScript)
        defer { collector.remove() }

        let result = try ProcessCollectorExecutor.run(collector.invocation, timeout: 20)

        #expect(result.exitCode == 0)
        #expect(result.standardOutput.utf8.count > 1_000_000)
        #expect(try CollectorStatus(json: Data(result.standardOutput.utf8)).eventCount == 3)
    }

    /// Swift Testing runs every test on the Swift concurrency pool: one thread
    /// per core, three on a GitHub macOS runner. When all of them are blocked,
    /// Dispatch starts no thread for DispatchQueue.global() work, so a status
    /// read must not need one. Twice as many blocking reads as cores fill it.
    @Test func statusReadsFinishWhileEveryPoolThreadIsBlocked() async throws {
        let collector = try FakeCollector(megabyteStatusScript)
        defer { collector.remove() }
        let callers = 2 * ProcessInfo.processInfo.activeProcessorCount

        let sizes = try await withThrowingTaskGroup(of: Int.self) { group in
            for _ in 0..<callers {
                group.addTask {
                    // Blocks this pool thread for the whole read, as a synchronous caller does.
                    try ProcessCollectorExecutor.run(collector.invocation, timeout: 20).standardOutput.utf8.count
                }
            }
            return try await group.reduce(into: [Int]()) { $0.append($1) }
        }

        #expect(sizes.count == callers)
        #expect(sizes.allSatisfy { $0 > 1_000_000 })
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

    @Test(arguments: [
        (200, #"{"ok":true}"#, true),
        (200, #"{"ok":true,"note":"extra fields are tolerated"}"#, true),
        (200, #"{"ok":false}"#, false),
        (200, #"{"ok":1}"#, false),
        (200, "OK", false),
        (401, #"{"ok":true}"#, false),
    ])
    func healthzReplyMustMatchTheCollectorContract(statusCode: Int, body: String, live: Bool) {
        #expect(CollectorClient.isHealthzReply(statusCode: statusCode, body: Data(body.utf8)) == live)
    }

    @Test func probeSeesTheCollectorHealthzAndSendsNoCredential() throws {
        let responder = try LoopbackResponder(reply: [
            "HTTP/1.1 200 OK", "content-type: application/json", "content-length: 11", "connection: close", "",
            #"{"ok":true}"#,
        ].joined(separator: "\r\n"))

        #expect(CollectorClient.defaultProbeLiveness(port: responder.port))
        let request = try #require(responder.request())
        #expect(request.hasPrefix("GET /healthz HTTP/1.1\r\n"))
        for header in ["x-plimsoll-token", "authorization", "cookie"] {
            #expect(!request.lowercased().contains(header))
        }
    }

    @Test func probeReportsStoppedForAnotherServiceOnThePort() throws {
        let responder = try LoopbackResponder(reply: [
            "HTTP/1.1 200 OK", "content-type: text/plain", "content-length: 2", "connection: close", "", "OK",
        ].joined(separator: "\r\n"))

        #expect(!CollectorClient.defaultProbeLiveness(port: responder.port))
    }

    @Test func probeReportsStoppedWhenNothingListens() throws {
        let port = try LoopbackResponder.unusedPort()
        let started = Date()

        #expect(!CollectorClient.defaultProbeLiveness(port: port))
        #expect(Date().timeIntervalSince(started) < CollectorClient.livenessTimeout)
    }

    @Test func probeNeverContactsPortsOutsideTheTCPRange() {
        #expect(!CollectorClient.defaultProbeLiveness(port: 0))
        #expect(!CollectorClient.defaultProbeLiveness(port: 70_000))
    }

    @Test func collectorErrorTextIsOneLineAndNeverACredential() {
        let token = String(repeating: "aB3_-", count: 8) + "xYz" // a 43-character base64url credential
        let hash = String(repeating: "0123456789abcdef", count: 4) // sha256 hex, not a credential
        #expect(token.count == 43)

        #expect(CollectorMessage.displayLine("Error: rejected x-plimsoll-token=\(token)\n    at main (cli.ts:1)")
            == "Error: rejected x-plimsoll-token=[redacted]")
        #expect(CollectorMessage.displayLine(#"{"managementRead":"\#(token)"}"#) == #"{"managementRead":"[redacted]"}"#)
        #expect(CollectorMessage.displayLine("\n\n  Error: home sha256:\(hash)  \n") == "Error: home sha256:\(hash)")
        #expect(CollectorMessage.displayLine(" \n ") == "no error output")
        let long = CollectorMessage.displayLine(String(repeating: "x ", count: 300))
        #expect(long.count == CollectorMessage.maximumLength + 1 && long.hasSuffix("…"))
    }

    @Test func failingCollectorCannotPutACredentialOnScreen() throws {
        let token = String(repeating: "Zz9-_", count: 8) + "q1W"
        let collector = try FakeCollector("""
            echo 'Error: management_credential_invalid \(token)' >&2
            echo '    at readDaemonState (cli.ts:1191)' >&2
            exit 1
            """)
        defer { collector.remove() }

        #expect {
            try CollectorClient(invocation: collector.invocation, probeLiveness: { _ in false }).snapshot()
        } throws: { error in
            let shown = error.localizedDescription
            return shown == "Collector exited with status 1: Error: management_credential_invalid [redacted]"
                && !shown.contains(token)
        }
    }

    @Test func sourcesNeverTouchTheCollectorCredential() throws {
        let sources = try packageSources()
        #expect(sources.count >= 6)
        for (file, text) in sources {
            for needle in ["local-ingest-auth", "managementRead", "x-plimsoll-token"] {
                #expect(!text.contains(needle), "\(file) mentions \(needle)")
            }
        }
    }

    /// What the permission doctor and README assert, checked in the source.
    @Test func sourcesUseNoPermissionPromptingAPIsOrBundleMetadata() throws {
        let promptingAPIs = [
            "AXIsProcessTrusted", // accessibility
            "AVCaptureDevice", "AVAudioSession", // camera, microphone
            "IOHIDRequestAccess", "CGEventTapCreate", "tapCreate", "addGlobalMonitorForEvents", // input monitoring
            "CGRequestScreenCaptureAccess", "SCShareableContent", "CGWindowListCreateImage", "CGDisplayStream", // screen
            "SMAppService", "SMLoginItemSetEnabled", "LaunchAgents", // helpers and LaunchAgents
        ]
        let sources = try packageSources()
        #expect(sources.count >= 6)
        for (file, text) in sources {
            for api in promptingAPIs {
                #expect(!text.contains(api), "\(file) uses \(api)")
            }
        }
        let bundleMetadata = try FileManager.default.subpathsOfDirectory(atPath: packageRoot.path)
            .filter { !$0.hasPrefix(".") && ($0.hasSuffix(".entitlements") || $0.hasSuffix(".plist")) }
        #expect(bundleMetadata.isEmpty)
    }

    @Test func menuLinesShowStateEventsCoverageAndTokens() throws {
        let status = try CollectorStatus(json: Data(#"{"port":49123,"stats":{"count":8,"tokenAttributedEvents":2,"totalInputTokens":100,"totalOutputTokens":50}}"#.utf8))
        let running = StatusLines(snapshot: CollectorSnapshot(running: true, status: status))
        #expect(running.summary == "Running · 8 events · 25.0% token coverage")
        #expect(running.tokens == "Tokens: 100 in · 50 out")
        #expect(running.json() == #"{"summary":"Running · 8 events · 25.0% token coverage","tokens":"Tokens: 100 in · 50 out"}"#)

        let empty = try CollectorStatus(json: Data(#"{"port":48271,"stats":null}"#.utf8))
        let stopped = StatusLines(snapshot: CollectorSnapshot(running: false, status: empty))
        #expect(stopped.summary == "Stopped · — events · — token coverage")
        #expect(stopped.tokens == "Tokens: — in · — out")

        let failed = StatusLines(error: CollectorClientError.timedOut(seconds: 60))
        #expect(failed.summary == "Collector unavailable")
        #expect(failed.tokens == "Collector status did not finish within 60 seconds.")
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

/// Status JSON of about 1 MB. The real document grows with the host, and
/// anything over the 64 KB pipe buffer blocks a collector nobody is reading.
private let megabyteStatusScript = """
    printf '{"port":48271,"stats":{"count":3,"tokenAttributedEvents":3},"padding":"'
    head -c 1000000 /dev/zero | tr '\\0' 'x'
    printf '"}'
    """

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

/// A one-shot HTTP responder on 127.0.0.1: it accepts one connection, keeps
/// the request head, sends a canned reply and closes.
private final class LoopbackResponder: @unchecked Sendable {
    let port: Int
    private let served = DispatchSemaphore(value: 0)
    // Written once before `served` is signalled; read only after waiting on it.
    private var head: String?

    init(reply: String) throws {
        let listener = try Self.listeningSocket()
        port = try Self.boundPort(listener)
        DispatchQueue.global().async {
            let connection = accept(listener, nil, nil)
            if connection >= 0 {
                var received = [UInt8]()
                var buffer = [UInt8](repeating: 0, count: 4096)
                while received.count < 16_384, !received.ends(with: Array("\r\n\r\n".utf8)) {
                    let count = read(connection, &buffer, buffer.count)
                    if count <= 0 { break }
                    received += buffer[..<count]
                }
                self.head = String(decoding: received, as: UTF8.self)
                _ = reply.withCString { write(connection, $0, strlen($0)) }
                close(connection)
            }
            close(listener)
            self.served.signal()
        }
    }

    /// The request head the probe sent, once the reply has gone out.
    func request() -> String? {
        served.wait(timeout: .now() + 5) == .success ? head : nil
    }

    /// A loopback port that was just free (bound, then released).
    static func unusedPort() throws -> Int {
        let socket = try listeningSocket()
        defer { close(socket) }
        return try boundPort(socket)
    }

    private static func listeningSocket() throws -> Int32 {
        let descriptor = socket(AF_INET, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw POSIXError(.EIO) }
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_addr.s_addr = inet_addr("127.0.0.1")
        let bound = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bound == 0, listen(descriptor, 1) == 0 else {
            close(descriptor)
            throw POSIXError(.EADDRNOTAVAIL)
        }
        return descriptor
    }

    private static func boundPort(_ descriptor: Int32) throws -> Int {
        var address = sockaddr_in()
        var length = socklen_t(MemoryLayout<sockaddr_in>.size)
        let result = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(descriptor, $0, &length) }
        }
        guard result == 0 else { throw POSIXError(.EINVAL) }
        return Int(UInt16(bigEndian: address.sin_port))
    }
}

private extension Array where Element == UInt8 {
    func ends(with suffix: [UInt8]) -> Bool {
        count >= suffix.count && Array(self[(count - suffix.count)...]) == suffix
    }
}

private let packageRoot = URL(fileURLWithPath: #filePath)
    .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()

/// Every Swift file under Sources/, as (relative path, contents).
private func packageSources() throws -> [(String, String)] {
    let sources = packageRoot.appendingPathComponent("Sources")
    return try FileManager.default.subpathsOfDirectory(atPath: sources.path)
        .filter { $0.hasSuffix(".swift") }
        .sorted()
        .map { ($0, try String(contentsOf: sources.appendingPathComponent($0), encoding: .utf8)) }
}
