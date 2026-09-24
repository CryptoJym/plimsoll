import CryptoKit
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
        #expect(summary.healthzKey == Data((0..<32).map { UInt8($0) }))
        #expect(summary.collectorVersion == "0.7.38")
    }

    @Test func summaryCountersMayBeNullButNeverNegative() throws {
        let empty = try StatusSummary(json: Data(summaryJSON(stats: "null").utf8))
        #expect(empty.eventCount == nil && empty.tokenCoveragePercent == nil)

        #expect(throws: SummaryProblem.unreadable) {
            try StatusSummary(json: Data(summaryJSON(count: -1).utf8))
        }
    }

    @Test(arguments: [
        summaryJSON(stats: "null").replacingOccurrences(of: "status-summary/v1", with: "status-summary/v2"),
        summaryJSON(stats: "null").replacingOccurrences(of: instanceA, with: "not-a-uuid"),
        summaryJSON(port: 0, stats: "null"),
        summaryJSON(stats: "null", updatedAt: "yesterday"),
        summaryJSON(stats: #"{"count":1.5,"tokenAttributedEvents":0,"totalInputTokens":0,"totalOutputTokens":0}"#),
        "not json",
        "[]",
    ])
    func summaryRejectsAnythingButTheCollectorsShape(json: String) {
        #expect(throws: SummaryProblem.unreadable) { try StatusSummary(json: Data(json.utf8)) }
    }

    /// Review should-fix (round 4): the decoder ignored collectorVersion and
    /// accepted unknown keys, so it did not match the exact schema it claimed.
    @Test(arguments: [
        summaryJSON().replacingOccurrences(of: #""collectorVersion":"0.7.38","#, with: ""),
        summaryJSON().replacingOccurrences(of: #""0.7.38""#, with: #""""#),
        summaryJSON().replacingOccurrences(of: #""0.7.38""#, with: "7"),
        summaryJSON().replacingOccurrences(of: #""stats""#, with: #""note":1,"stats""#),
        summaryJSON().replacingOccurrences(of: #""healthzKey":"\#(Vector.key)","#, with: ""),
        summaryJSON(key: String(Vector.key.dropLast())),
        summaryJSON(key: String(Vector.key.dropLast()) + "+"),
        summaryJSON(stats: #"{"count":1,"tokenAttributedEvents":0,"totalInputTokens":0,"totalOutputTokens":0,"extra":2}"#),
        summaryJSON(stats: #"{"count":1}"#),
        summaryJSON(stats: "[]"),
    ])
    func summaryDecoderMatchesTheExactSchema(json: String) {
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
        let summary = try SummaryFile.read(home: home.url).get()
        #expect(CollectorMonitor.state(home: home.url, now: written + 600, isLive: { _ in true })
            == .notUpdating(summary, age: 600))
        #expect(CollectorMonitor.state(home: home.url, now: written + 7_200, isLive: { _ in false })
            == .stopped(summary, age: 7_200))
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
            summaryJSON().replacingOccurrences(of: instanceA, with: token),
            summaryJSON().replacingOccurrences(of: #""stats""#, with: #""note":"x\#(token)y\n\#(token)","stats""#),
            summaryJSON().replacingOccurrences(of: #""plimsoll.status-summary/v1""#, with: #""\#(token)""#),
            summaryJSON().replacingOccurrences(of: #""0.7.38""#, with: #""\#(token)""#),
            "Error: \(token)\n\(token.prefix(21))\n\(token.suffix(22))",
            // A valid summary: its own 43-character healthzKey is never shown.
            summaryJSON(count: 5),
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
            #expect(!line.contains(Vector.key))
        }
    }

    // MARK: Liveness is proven by the run that wrote the summary

    /// The collector's proof:status-summary pins the same vector, so the app
    /// and the collector compute the same HMAC over the same message.
    @Test func healthzProofMatchesTheCollectorsTestVector() throws {
        #expect(collectorProof(challenge: Vector.challenge, port: Vector.port) == Vector.proof)
        let summary = try StatusSummary(json: Data(summaryJSON(port: Vector.port).utf8))
        let reply = Data(#"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(Vector.proof)"}"#.utf8)
        #expect(LivenessProbe.isCollectorReply(statusCode: 200, body: reply, summary: summary, challenge: Vector.challenge))
    }

    /// Review blocker (round 4): `{"ok":true,"instanceId":<the summary's id>}`
    /// was enough, and that id is public. Only the HMAC of this challenge under
    /// the summary's key, for this port, names the run now.
    @Test func onlyTheCollectorsProofOfThisChallengeNamesThisRun() throws {
        let summary = try StatusSummary(json: Data(summaryJSON(port: Vector.port).utf8))
        let proof = Vector.proof
        let accepted = [
            #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof)"}"#,
            #"{"proof":"\#(proof)","instanceId":"\#(instanceA)","ok":true}"#,
        ]
        let refused: [(String, Int, String)] = [
            ("the public id alone (the round-3 reply)", 200, #"{"ok":true,"instanceId":"\#(instanceA)"}"#),
            ("any other service's common health reply", 200, #"{"ok":true}"#),
            ("another collector run", 200, #"{"ok":true,"instanceId":"\#(instanceB)","proof":"\#(proof)"}"#),
            ("an earlier answer to another challenge", 200,
             #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(collectorProof(challenge: otherChallenge, port: Vector.port))"}"#),
            ("an answer relayed from another port", 200,
             #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(collectorProof(challenge: Vector.challenge, port: Vector.port + 1))"}"#),
            ("a proof under another key", 200,
             #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(collectorProof(challenge: Vector.challenge, port: Vector.port, key: otherKey))"}"#),
            ("not the exact shape", 200, #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof)","note":1}"#),
            ("ok is not the boolean true", 200, #"{"ok":1,"instanceId":"\#(instanceA)","proof":"\#(proof)"}"#),
            ("ok is false", 200, #"{"ok":false,"instanceId":"\#(instanceA)","proof":"\#(proof)"}"#),
            ("a truncated proof", 200, #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof.dropLast())"}"#),
            ("a proof that is not base64url", 200, #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof.dropLast())+"}"#),
            ("an error status", 401, #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof)"}"#),
            ("not JSON", 200, "OK"),
        ]
        for body in accepted {
            #expect(LivenessProbe.isCollectorReply(statusCode: 200, body: Data(body.utf8), summary: summary, challenge: Vector.challenge))
        }
        for (why, statusCode, body) in refused {
            let named = LivenessProbe.isCollectorReply(
                statusCode: statusCode, body: Data(body.utf8), summary: summary, challenge: Vector.challenge
            )
            #expect(!named, "\(why)")
        }
    }

    @Test func probeRecognisesTheCollectorRunAndSendsNoCredentialOrKey() throws {
        let responder = try LoopbackResponder(respond: collector)
        let summary = try StatusSummary(json: Data(summaryJSON(port: responder.port).utf8))

        #expect(LivenessProbe.answers(for: summary))
        let request = try #require(responder.request())
        #expect(challenge(in: request) != nil)
        #expect(!request.contains(Vector.key))
        for header in ["x-plimsoll-token", "authorization", "cookie"] {
            #expect(!request.lowercased().contains(header))
        }
    }

    @Test func eachProbeSendsAFreshChallenge() throws {
        var sent: [String] = []
        for _ in 0..<2 {
            let responder = try LoopbackResponder(respond: collector)
            let summary = try StatusSummary(json: Data(summaryJSON(port: responder.port).utf8))
            #expect(LivenessProbe.answers(for: summary))
            let request = try #require(responder.request())
            sent.append(try #require(challenge(in: request)))
        }
        #expect(Set(sent).count == 2)
    }

    /// Review finding B6, then the round-4 blocker: a non-collector loopback
    /// service on the summary's port was shown as Running, and Open Dashboard
    /// would have handed it the collector's origin. That includes one that
    /// replays the public instanceId or an earlier proof.
    @Test(arguments: [
        #"{"ok":true}"#,
        #"{"ok":true,"instanceId":"\#(instanceB)"}"#,
        #"{"ok":true,"instanceId":"\#(instanceA)"}"#,
        #"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(Vector.proof)"}"#,
    ])
    func anotherLoopbackServiceIsNotTheCollector(reply: String) throws {
        let responder = try LoopbackResponder(reply: healthzReply(reply))
        let home = try TemporaryHome()
        defer { home.remove() }
        try home.writeSummary(summaryJSON(port: responder.port, count: 7, updatedAt: isoNow()))

        let state = CollectorMonitor.state(home: home.url)
        guard case .stopped = state else {
            Issue.record("\(state) is not Stopped"); return
        }
        #expect(StatusLines(state: state).dashboard == nil)
        #expect(responder.request() != nil)
    }

    /// Round-4 blocker, as the review reproduced it: a process that knows the
    /// public instanceId and answers each challenge, but not with the file's key.
    @Test func aResponderWithoutTheKeyIsNotTheCollector() throws {
        let impostor = try LoopbackResponder(respond: { head, port in
            let proof = collectorProof(challenge: challenge(in: head) ?? "", port: port, key: otherKey)
            return healthzReply(#"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof)"}"#)
        })
        let home = try TemporaryHome()
        defer { home.remove() }
        try home.writeSummary(summaryJSON(port: impostor.port, count: 7, updatedAt: isoNow()))

        let state = CollectorMonitor.state(home: home.url)
        guard case .stopped = state else {
            Issue.record("\(state) is not Stopped"); return
        }
        #expect(StatusLines(state: state).dashboard == nil)
        #expect(impostor.request() != nil)
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
                    let responder = try LoopbackResponder(respond: collector)
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

    @Test func probeReportsStoppedWhenNothingListens() throws {
        let port = try LoopbackResponder.unusedPort()
        let summary = try StatusSummary(json: Data(summaryJSON(port: port).utf8))
        let started = Date()

        #expect(!LivenessProbe.answers(for: summary))
        #expect(Date().timeIntervalSince(started) < LivenessProbe.timeout)
    }

    @Test func probeNeverContactsPortsOutsideTheTCPRange() {
        let key = Data((0..<32).map { UInt8($0) })
        #expect(!LivenessProbe.answers(port: 0, instanceId: instanceA, key: key))
        #expect(!LivenessProbe.answers(port: 70_000, instanceId: instanceA, key: key))
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

    /// Review should-fix: the Start/Stop mutation passed every test because
    /// only the invocation was checked. Now any string in the sources naming
    /// a collector command, or any menu action beyond Refresh, Open Dashboard
    /// and Quit, fails here.
    @Test func sourcesOfferNoWayToChangeTheCollector() throws {
        let commandWords: Set<String> = ["start", "stop", "restart", "setup", "install", "uninstall", "load",
                                         "unload", "rotate", "purge", "join", "upload", "sync", "kill"]
        let literal = try NSRegularExpression(pattern: #""(?:[^"\\\n]|\\.)*""#)
        let sources = try packageSources()
        #expect(sources.count >= 5)
        for (file, text) in sources {
            let range = NSRange(text.startIndex..., in: text)
            for match in literal.matches(in: text, range: range) {
                let string = String(text[Range(match.range, in: text)!])
                let words = Set(string.lowercased().split(whereSeparator: { !$0.isLetter }).map(String.init))
                #expect(words.isDisjoint(with: commandWords), "\(file): \(string)")
            }
        }

        let app = try #require(sources.first { $0.0.hasSuffix("AppDelegate.swift") }?.1)
        let selector = try NSRegularExpression(pattern: #"#selector\((\w+)\)"#)
        let actions = selector.matches(in: app, range: NSRange(app.startIndex..., in: app))
            .map { String(app[Range($0.range(at: 1), in: app)!]) }
        #expect(Set(actions) == ["refreshAction", "openDashboardAction", "quitAction"])
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

/// The shared /healthz proof test vector; the collector's proof:status-summary
/// pins the same values. The key is bytes 0...31.
private enum Vector {
    static let key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
    static let port = 49123
    static let challenge = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8"
    static let proof = "lnozo0nCndram-7O1AV5MwmsEMnovjQYlD58Bw5wX0U"
}

/// Another run's key (bytes 100...131) and another challenge.
private let otherKey = "ZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1-f4CBgoM"
private let otherChallenge = String(repeating: "A", count: 43)

/// A summary exactly as the collector writes it, written at 1_790_000_000.
private func summaryJSON(
    port: Int = 49123, count: Int = 0, tokenEvents: Int = 0, input: Int = 0, output: Int = 0, stats: String? = nil,
    updatedAt: String = "2026-09-21T14:13:20.000Z", key: String = Vector.key
) -> String {
    let counters = stats ?? #"{"count":\#(count),"tokenAttributedEvents":\#(tokenEvents),"totalInputTokens":\#(input),"totalOutputTokens":\#(output)}"#
    return #"{"schema":"plimsoll.status-summary/v1","instanceId":"\#(instanceA)","healthzKey":"\#(key)","collectorVersion":"0.7.38","port":\#(port),"updatedAt":"\#(updatedAt)","stats":\#(counters)}"#
}

/// The collector's /healthz proof, computed here with CryptoKit apart from
/// the app's code (the test vector pins it).
private func collectorProof(challenge: String, port: Int, instanceId: String = instanceA, key: String = Vector.key) -> String {
    let keyBytes = Data(base64Encoded: key.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/") + "=")!
    let message = Data("plimsoll.healthz-proof/v1\n\(port)\n\(instanceId)\n\(challenge)".utf8)
    let code = HMAC<SHA256>.authenticationCode(for: message, using: SymmetricKey(data: keyBytes))
    return Data(code).base64EncodedString()
        .replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
}

/// The challenge a probe's request head carries, if it is the exact form.
private func challenge(in head: String) -> String? {
    guard let line = head.range(of: #"^GET /healthz\?challenge=[A-Za-z0-9_-]{43} HTTP/1\.1\r\n"#, options: .regularExpression)
    else { return nil }
    return String(head[line].dropFirst("GET /healthz?challenge=".count).prefix(43))
}

/// How the collector run holding the fixture key answers a probe.
private let collector: @Sendable (String, Int) -> String = { head, port in
    let proof = collectorProof(challenge: challenge(in: head) ?? "", port: port)
    return healthzReply(#"{"ok":true,"instanceId":"\#(instanceA)","proof":"\#(proof)"}"#)
}

private func isoNow() -> String {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.string(from: Date())
}

private let instanceB = "7c2d4e6f-8a9b-4c1d-8e2f-3a4b5c6d7e8f"

/// An HTTP response carrying a JSON body, as the collector's /healthz sends it.
private func healthzReply(_ body: String) -> String {
    [
        "HTTP/1.1 200 OK", "content-type: application/json", "content-length: \(body.utf8.count)",
        "connection: close", "", body,
    ].joined(separator: "\r\n")
}

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
/// the request head, sends a reply made from the head and its port, and closes.
private final class LoopbackResponder: @unchecked Sendable {
    let port: Int
    private let served = DispatchSemaphore(value: 0)
    // Written once before `served` is signalled; read only after waiting on it.
    private var head: String?

    convenience init(reply: String) throws {
        try self.init(respond: { _, _ in reply })
    }

    init(respond: @escaping @Sendable (_ head: String, _ port: Int) -> String) throws {
        let listener = try Self.listeningSocket()
        let port = try Self.boundPort(listener)
        self.port = port
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
                let head = String(decoding: received, as: UTF8.self)
                self.head = head
                _ = respond(head, port).withCString { write(connection, $0, strlen($0)) }
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
