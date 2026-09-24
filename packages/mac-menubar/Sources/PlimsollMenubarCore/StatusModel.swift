import Foundation

/// What the menu shows, from the summary file and the liveness check.
public enum CollectorState: Equatable, Sendable {
    /// The run that wrote the summary answers on its port, and it is fresh.
    case running(StatusSummary)
    /// The collector answers, but has not rewritten its summary lately.
    case notUpdating(StatusSummary, age: TimeInterval)
    /// Nothing answers as that collector; the summary is the last it wrote.
    case stopped(StatusSummary, age: TimeInterval)
    /// No usable summary.
    case unavailable(SummaryProblem)
}

public enum CollectorMonitor {
    /// Reads the summary, then asks whether the collector on its port is the
    /// run that wrote it (its /healthz names the same instanceId).
    public static func state(
        home: URL?,
        now: Date = Date(),
        isLive: (StatusSummary) -> Bool = { LivenessProbe.answers(as: $0.instanceId, port: $0.port) }
    ) -> CollectorState {
        switch SummaryFile.read(home: home) {
        case let .failure(problem):
            return .unavailable(problem)
        case let .success(summary):
            let age = max(0, now.timeIntervalSince(summary.updatedAt))
            guard isLive(summary) else { return .stopped(summary, age: age) }
            return age > StatusSummary.staleAfter ? .notUpdating(summary, age: age) : .running(summary)
        }
    }
}

/// The two status lines the menu shows, and the dashboard it may offer.
/// Every line is fixed text around numbers; nothing read from disk, the
/// environment or the network is shown as text. `--status` prints the same.
public struct StatusLines: Equatable, Sendable {
    public let summary: String
    public let tokens: String
    /// Offered only while the collector is verified running: the dashboard
    /// keeps its credential in that origin's storage, so no other service
    /// may be handed it.
    public let dashboard: URL?

    public init(state: CollectorState) {
        switch state {
        case let .running(summary):
            self.summary = "Running · \(Self.counts(summary))"
            tokens = Self.tokens(summary)
            dashboard = URL(string: "http://127.0.0.1:\(summary.port)/")
        case let .notUpdating(summary, age):
            self.summary = "Collector unavailable · summary not updated for \(Self.age(age))"
            tokens = "\(Self.tokens(summary)) · as of \(Self.age(age)) ago"
            dashboard = nil
        case let .stopped(summary, age):
            self.summary = "Stopped · \(Self.counts(summary))"
            tokens = "\(Self.tokens(summary)) · as of \(Self.age(age)) ago"
            dashboard = nil
        case let .unavailable(problem):
            summary = "Collector unavailable"
            tokens = problem.message
            dashboard = nil
        }
    }

    /// One JSON object: `{"dashboard":…|null,"summary":…,"tokens":…}`.
    public func json() -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let fields: [String: String?] = ["summary": summary, "tokens": tokens, "dashboard": dashboard?.absoluteString]
        let data = (try? encoder.encode(fields)) ?? Data()
        return String(decoding: data, as: UTF8.self)
    }

    private static func counts(_ summary: StatusSummary) -> String {
        let count = summary.eventCount.map(String.init) ?? "—"
        let coverage = summary.tokenCoveragePercent.map { String(format: "%.1f%%", $0) } ?? "—"
        return "\(count) events · \(coverage) token coverage"
    }

    private static func tokens(_ summary: StatusSummary) -> String {
        let input = summary.totalInputTokens.map(String.init) ?? "—"
        let output = summary.totalOutputTokens.map(String.init) ?? "—"
        return "Tokens: \(input) in · \(output) out"
    }

    static func age(_ seconds: TimeInterval) -> String {
        switch seconds {
        case ..<60: return "\(Int(seconds)) s"
        case ..<3_600: return "\(Int(seconds / 60)) min"
        case ..<86_400: return "\(Int(seconds / 3_600)) h"
        default: return "\(Int(seconds / 86_400)) d"
        }
    }
}
