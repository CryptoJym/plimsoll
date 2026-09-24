import Foundation
import Testing
@testable import PlimsollMenubarCore

struct PlimsollMenubarCoreTests {
    // MARK: Where the summary lives

    @Test func homeDefaultsToTheCollectorsApplicationSupportFolder() {
        let home = CollectorHome.resolve(environment: [:], userHome: "/Users/someone")
        #expect(home?.path == "/Users/someone/Library/Application Support/Plimsoll")
        #expect(CollectorHome.resolve(environment: ["PLIMSOLL_HOME": "  "], userHome: "/Users/someone") == home)
    }

    @Test func homeHonoursAnAbsolutePlimsollHomeAndRefusesARelativeOne() {
        #expect(CollectorHome.resolve(environment: ["PLIMSOLL_HOME": "/srv/plimsoll"])?.path == "/srv/plimsoll")
        #expect(CollectorHome.resolve(environment: ["PLIMSOLL_HOME": "relative/home"]) == nil)
        #expect(SummaryFile.read(home: nil) == .failure(.invalidHome))
    }

    // MARK: Parsing

    @Test func summaryParsesTheCollectorsFile() throws {
        let summary = try StatusSummary(json: Data(summaryJSON(count: 8, tokenEvents: 2, input: 100, output: 50).utf8))

        #expect(summary.instanceId == instanceA)
        #expect(summary.port == 49123)
        #expect(summary.eventCount == 8)
        #expect(summary.tokenAttributedEvents == 2)
        #expect(summary.totalInputTokens == 100)
        #expect(summary.totalOutputTokens == 50)
        #expect(summary.tokenCoveragePercent == 25)
        #expect(summary.updatedAt == Date(timeIntervalSince1970: 1_790_000_000))
    }

    @Test func summaryCountersMayBeNullButNeverNegative() throws {
        let empty = try StatusSummary(json: Data(summaryJSON(stats: "null").utf8))
        #expect(empty.eventCount == nil && empty.tokenCoveragePercent == nil)

        #expect(throws: SummaryProblem.unreadable) {
            try StatusSummary(json: Data(summaryJSON(count: -1).utf8))
        }
    }

    @Test(arguments: [
        #"{"schema":"plimsoll.status-summary/v2","instanceId":"\#(instanceA)","port":49123,"updatedAt":"2026-09-21T14:13:20.000Z","stats":null}"#,
        #"{"schema":"plimsoll.status-summary/v1","instanceId":"not-a-uuid","port":49123,"updatedAt":"2026-09-21T14:13:20.000Z","stats":null}"#,
        #"{"schema":"plimsoll.status-summary/v1","instanceId":"\#(instanceA)","port":0,"updatedAt":"2026-09-21T14:13:20.000Z","stats":null}"#,
        #"{"schema":"plimsoll.status-summary/v1","instanceId":"\#(instanceA)","port":49123,"updatedAt":"yesterday","stats":null}"#,
        #"{"schema":"plimsoll.status-summary/v1","instanceId":"\#(instanceA)","port":49123,"updatedAt":"2026-09-21T14:13:20.000Z","stats":{"count":1.5}}"#,
        "not json",
        "[]",
    ])
    func summaryRejectsAnythingButTheCollectorsShape(json: String) {
        #expect(throws: SummaryProblem.unreadable) { try StatusSummary(json: Data(json.utf8)) }
    }

    // MARK: Reading the file

    @Test func readingFindsTheUsersOwnPrivateSummary() throws {
        let home = try TemporaryHome()
        defer { home.remove() }
        try home.writeSummary(summaryJSON(count: 3))

        #expect(try SummaryFile.read(home: home.url).get().eventCount == 3)
    }

    @Test func readingReportsAMissingSummary() throws {
        let home = try TemporaryHome()
        defer { home.remove() }

        #expect(SummaryFile.read(home: home.url) == .failure(.missing))
    }

    @Test(arguments: [0o644, 0o640, 0o604, 0o660])
    func readingRefusesASummaryOthersCanRead(mode: Int) throws {
        let home = try TemporaryHome()
        defer { home.remove() }
        try home.writeSummary(summaryJSON(), mode: mode_t(mode))

        #expect(SummaryFile.read(home: home.url) == .failure(.notPrivate))
    }

    @Test func readingRefusesASymlinkedSummary() throws {
        let home = try TemporaryHome()
        defer { home.remove() }
        let elsewhere = home.url.appendingPathComponent("elsewhere.json")
        try Data(summaryJSON().utf8).write(to: elsewhere)
        chmod(elsewhere.path, 0o600)
        try FileManager.default.createSymbolicLink(
            at: home.url.appendingPathComponent(StatusSummary.fileName), withDestinationURL: elsewhere
        )

        #expect(SummaryFile.read(home: home.url) == .failure(.notPrivate))
    }

    @Test func readingNeverBlocksOnAFifoInPlaceOfTheSummary() throws {
        let home = try TemporaryHome()
        defer { home.remove() }
        #expect(mkfifo(home.url.appendingPathComponent(StatusSummary.fileName).path, 0o600) == 0)

        #expect(SummaryFile.read(home: home.url) == .failure(.notPrivate))
    }

    @Test func readingRefusesAnOversizedOrMalformedSummary() throws {
        let home = try TemporaryHome()
        defer { home.remove() }
        try home.writeSummary(String(repeating: " ", count: StatusSummary.maximumBytes + 1) + summaryJSON())
        #expect(SummaryFile.read(home: home.url) == .failure(.unreadable))

        try home.writeSummary("{\"schema\":")
        #expect(SummaryFile.read(home: home.url) == .failure(.unreadable))
    }

    // MARK: What the menu shows

    @Test func stateFollowsLivenessAndFreshness() throws {
        let home = try TemporaryHome()
        defer { home.remove() }
        try home.writeSummary(summaryJSON(count: 4))
        let written = Date(timeIntervalSince1970: 1_790_000_000)

        guard case .running = CollectorMonitor.state(home: home.url, now: written + 5, isLive: { _ in true }) else {
            Issue.record("a live collector with a fresh summary is running"); return
        }
        #expect(CollectorMonitor.state(home: home.url, now: written + 600, isLive: { _ in true })
            == .notUpdating(try SummaryFile.read(home: home.url).get(), age: 600))
        #expect(CollectorMonitor.state(home: home.url, now: written + 7_200, isLive: { _ in false })
            == .stopped(try SummaryFile.read(home: home.url).get(), age: 7_200))
        #expect(CollectorMonitor.state(home: nil, isLive: { _ in true }) == .unavailable(.invalidHome))
    }

    @Test func menuLinesForEachState() throws {
        let summary = try StatusSummary(json: Data(summaryJSON(count: 8, tokenEvents: 2, input: 100, output: 50).utf8))

        let running = StatusLines(state: .running(summary))
        #expect(running.summary == "Running · 8 events · 25.0% token coverage")
        #expect(running.tokens == "Tokens: 100 in · 50 out")
        #expect(running.dashboard?.absoluteString == "http://127.0.0.1:49123/")
        #expect(running.json() == #"{"dashboard":"http://127.0.0.1:49123/","summary":"Running · 8 events · 25.0% token coverage","tokens":"Tokens: 100 in · 50 out"}"#)

        let stopped = StatusLines(state: .stopped(summary, age: 7_200))
        #expect(stopped.summary == "Stopped · 8 events · 25.0% token coverage")
        #expect(stopped.tokens == "Tokens: 100 in · 50 out · as of 2 h ago")
        #expect(stopped.dashboard == nil)

        let stale = StatusLines(state: .notUpdating(summary, age: 420))
        #expect(stale.summary == "Collector unavailable · summary not updated for 7 min")
        #expect(stale.dashboard == nil)

        let missing = StatusLines(state: .unavailable(.missing))
        #expect(missing.summary == "Collector unavailable" && missing.tokens == "No status summary found")
        #expect(missing.json() == #"{"dashboard":null,"summary":"Collector unavailable","tokens":"No status summary found"}"#)
    }

    /// Credential display fails closed: the menu never echoes text from the
    /// file, the environment or the network, so a token planted in any of
    /// them (whole, embedded in a longer run, or split across lines) cannot
    /// reach the screen.
    @Test func menuTextNeverCarriesACredentialShapedRun() throws {
        let token = String(repeating: "aB3_-", count: 8) + "xYz"
        let home = try TemporaryHome(named: "home-\(token)")
        defer { home.remove() }
        let planted = [
            #"{"schema":"plimsoll.status-summary/v1","instanceId":"\#(token)","port":49123,"updatedAt":"2026-09-21T14:13:20.000Z","stats":null}"#,
            summaryJSON().replacingOccurrences(of: #""stats""#, with: #""note":"x\#(token)y\n\#(token)","stats""#),
            summaryJSON().replacingOccurrences(of: #""plimsoll.status-summary/v1""#, with: #""\#(token)""#),
            "Error: \(token)\n\(token.prefix(21))\n\(token.suffix(22))",
        ]
        var shown: [String] = []
        for text in planted {
            try home.writeSummary(text)
            for live in [true, false] {
                let lines = StatusLines(state: CollectorMonitor.state(home: home.url, isLive: { _ in live }))
                shown += [lines.summary, lines.tokens, lines.json()]
            }
        }
        shown += [StatusLines(state: .unavailable(.invalidHome)).tokens]

        #expect(!shown.isEmpty)
        for line in shown {
            #expect(line.range(of: "[A-Za-z0-9_-]{20,}", options: .regularExpression) == nil, "\(line)")
        }
    }

    // MARK: Liveness

    @Test(arguments: [
        (200, #"{"ok":true}"#, true),
        (200, #"{"ok":true,"note":"extra fields are tolerated"}"#, true),
        (200, #"{"ok":false}"#, false),
        (200, #"{"ok":1}"#, false),
        (200, "OK", false),
        (401, #"{"ok":true}"#, false),
    ])
    func healthzReplyMustMatchTheCollectorContract(statusCode: Int, body: String, live: Bool) {
        #expect(LivenessProbe.isHealthzReply(statusCode: statusCode, body: Data(body.utf8)) == live)
    }

    @Test func probeSeesTheCollectorHealthzAndSendsNoCredential() throws {
        let responder = try LoopbackResponder(reply: collectorHealthzReply)

        #expect(LivenessProbe.healthz(port: responder.port))
        let request = try #require(responder.request())
        #expect(request.hasPrefix("GET /healthz HTTP/1.1\r\n"))
        for header in ["x-plimsoll-token", "authorization", "cookie"] {
            #expect(!request.lowercased().contains(header))
        }
    }

    /// Swift Testing runs every test on the Swift concurrency pool: one thread
    /// per core, three on a GitHub macOS runner. When all of them are blocked,
    /// Dispatch starts no thread for DispatchQueue.global() work, so a status
    /// read must not need one. Twice as many blocking reads as cores fill it.
    @Test func statusReadsFinishWhileEveryPoolThreadIsBlocked() async throws {
        let callers = 2 * ProcessInfo.processInfo.activeProcessorCount

        let running = try await withThrowingTaskGroup(of: Bool.self) { group in
            for _ in 0..<callers {
                group.addTask {
                    let responder = try LoopbackResponder(reply: collectorHealthzReply)
                    let home = try TemporaryHome()
                    defer { home.remove() }
                    try home.writeSummary(summaryJSON(port: responder.port, updatedAt: isoNow()))
                    // Blocks this pool thread for the whole file read and probe.
                    guard case .running = CollectorMonitor.state(home: home.url) else { return false }
                    return responder.request() != nil
                }
            }
            return try await group.reduce(into: [Bool]()) { $0.append($1) }
        }

        #expect(running.count == callers)
        #expect(running.allSatisfy { $0 })
    }

    @Test func probeReportsStoppedForAnotherServiceOnThePort() throws {
        let responder = try LoopbackResponder(reply: [
            "HTTP/1.1 200 OK", "content-type: text/plain", "content-length: 2", "connection: close", "", "OK",
        ].joined(separator: "\r\n"))

        #expect(!LivenessProbe.healthz(port: responder.port))
    }

    @Test func probeReportsStoppedWhenNothingListens() throws {
        let port = try LoopbackResponder.unusedPort()
        let started = Date()

        #expect(!LivenessProbe.healthz(port: port))
        #expect(Date().timeIntervalSince(started) < LivenessProbe.timeout)
    }

    @Test func probeNeverContactsPortsOutsideTheTCPRange() {
        #expect(!LivenessProbe.healthz(port: 0))
        #expect(!LivenessProbe.healthz(port: 70_000))
    }

    // MARK: What the sources can do

    /// The app reads a file and makes one loopback request. It cannot start a
    /// process, so it never runs `plimsoll status` (which opens the ledger and
    /// reads the management credential), never leaves a child behind on a
    /// timeout, and never captures child output to display.
    @Test func sourcesStartNoProcess() throws {
        let sources = try packageSources()
        #expect(sources.count >= 5)
        let spawning = ["Process(", "NSTask", "posix_spawn", "fork(", "execv", "execl", "system(", "popen(",
                        "/usr/bin/env", "launchctl", "NSAppleScript", "\"status\""]
        for (file, text) in sources {
            for api in spawning {
                #expect(!text.contains(api), "\(file) uses \(api)")
            }
        }
    }

    @Test func sourcesNeverTouchTheCollectorCredential() throws {
        let sources = try packageSources()
        #expect(sources.count >= 5)
        for (file, text) in sources {
            for needle in ["local-ingest-auth", "managementRead", "x-plimsoll-token", "work-ledger"] {
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
        #expect(sources.count >= 5)
        for (file, text) in sources {
            for api in promptingAPIs {
                #expect(!text.contains(api), "\(file) uses \(api)")
            }
        }
        let bundleMetadata = try FileManager.default.subpathsOfDirectory(atPath: packageRoot.path)
            .filter { !$0.hasPrefix(".") && ($0.hasSuffix(".entitlements") || $0.hasSuffix(".plist")) }
        #expect(bundleMetadata.isEmpty)
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

        let json = try PermissionDoctor.json()
        #expect(json.contains("No additional macOS permissions requested"))
        #expect(json.contains("\"requestsAdditionalPermissions\":false"))
    }
}

private let instanceA = "0f5b9a52-3c1e-4a8b-9d2e-6f7a8b9c0d1e"

/// A summary exactly as the collector writes it, written at 1_790_000_000.
private func summaryJSON(
    port: Int = 49123, count: Int = 0, tokenEvents: Int = 0, input: Int = 0, output: Int = 0, stats: String? = nil,
    updatedAt: String = "2026-09-21T14:13:20.000Z"
) -> String {
    let counters = stats ?? #"{"count":\#(count),"tokenAttributedEvents":\#(tokenEvents),"totalInputTokens":\#(input),"totalOutputTokens":\#(output)}"#
    return #"{"schema":"plimsoll.status-summary/v1","instanceId":"\#(instanceA)","collectorVersion":"0.7.38","port":\#(port),"updatedAt":"\#(updatedAt)","stats":\#(counters)}"#
}

private func isoNow() -> String {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.string(from: Date())
}

/// The collector's `/healthz` answer before this change: `{"ok":true}` only.
private let collectorHealthzReply = [
    "HTTP/1.1 200 OK", "content-type: application/json", "content-length: 11", "connection: close", "",
    #"{"ok":true}"#,
].joined(separator: "\r\n")

/// A private (0700) directory standing in for the collector home.
private struct TemporaryHome: Sendable {
    let url: URL

    init(named name: String = "home") throws {
        url = FileManager.default.temporaryDirectory
            .appendingPathComponent("plimsoll-menubar-test-\(UUID().uuidString)")
            .appendingPathComponent(name)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true,
                                                attributes: [.posixPermissions: 0o700])
    }

    func writeSummary(_ text: String, mode: mode_t = 0o600) throws {
        let file = url.appendingPathComponent(StatusSummary.fileName)
        try? FileManager.default.removeItem(at: file)
        try Data(text.utf8).write(to: file)
        chmod(file.path, mode)
    }

    func remove() {
        try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
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
        // Its own thread, not DispatchQueue.global(): a test that blocks its
        // pool thread on the probe must not also wait on global-queue work
        // (see statusReadsFinishWhileEveryPoolThreadIsBlocked). It waits at
        // most 10 s for the probe to connect.
        Thread {
            var waiting = pollfd(fd: listener, events: Int16(POLLIN), revents: 0)
            let connection = poll(&waiting, 1, 10_000) > 0 ? accept(listener, nil, nil) : -1
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
        }.start()
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
