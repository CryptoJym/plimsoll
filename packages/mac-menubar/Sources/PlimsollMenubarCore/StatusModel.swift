import Foundation

public struct CollectorStatus: Equatable, Sendable {
    public let port: Int
    public let eventCount: Int?
    public let tokenAttributedEvents: Int?
    public let totalInputTokens: Int?
    public let totalOutputTokens: Int?

    public var tokenCoveragePercent: Double? {
        guard let eventCount, eventCount > 0,
              let tokenAttributedEvents, tokenAttributedEvents >= 0 else {
            return nil
        }
        return (Double(tokenAttributedEvents) / Double(eventCount)) * 100
    }

    public init(json: Data) throws {
        let wire = try JSONDecoder().decode(WireStatus.self, from: json)
        self.port = (wire.port ?? 48271) > 0 && (wire.port ?? 48271) <= 65535
            ? wire.port ?? 48271
            : 48271
        self.eventCount = wire.stats?.count
        self.tokenAttributedEvents = wire.stats?.tokenAttributedEvents
        self.totalInputTokens = wire.stats?.totalInputTokens
        self.totalOutputTokens = wire.stats?.totalOutputTokens
    }

    private struct WireStatus: Decodable {
        let port: Int?
        let stats: WireStats?
    }

    private struct WireStats: Decodable {
        let count: Int?
        let tokenAttributedEvents: Int?
        let totalInputTokens: Int?
        let totalOutputTokens: Int?
    }
}

public struct CollectorSnapshot: Equatable, Sendable {
    public let running: Bool
    public let port: Int
    public let eventCount: Int?
    public let tokenAttributedEvents: Int?
    public let totalInputTokens: Int?
    public let totalOutputTokens: Int?
    public let tokenCoveragePercent: Double?

    public init(running: Bool, status: CollectorStatus) {
        self.running = running
        self.port = status.port
        self.eventCount = status.eventCount
        self.tokenAttributedEvents = status.tokenAttributedEvents
        self.totalInputTokens = status.totalInputTokens
        self.totalOutputTokens = status.totalOutputTokens
        self.tokenCoveragePercent = status.tokenCoveragePercent
    }
}

/// The two status lines the menu shows. `--status` prints the same text.
public struct StatusLines: Equatable, Sendable {
    public let summary: String
    public let tokens: String

    public init(snapshot: CollectorSnapshot) {
        let state = snapshot.running ? "Running" : "Stopped"
        let count = snapshot.eventCount.map(String.init) ?? "—"
        let coverage = snapshot.tokenCoveragePercent.map { String(format: "%.1f%%", $0) } ?? "—"
        summary = "\(state) · \(count) events · \(coverage) token coverage"

        let input = snapshot.totalInputTokens.map(String.init) ?? "—"
        let output = snapshot.totalOutputTokens.map(String.init) ?? "—"
        tokens = "Tokens: \(input) in · \(output) out"
    }

    public init(error: Error) {
        summary = "Collector unavailable"
        tokens = error.localizedDescription
    }

    /// One JSON object, `{"summary":…,"tokens":…}`.
    public func json() -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let data = (try? encoder.encode(["summary": summary, "tokens": tokens])) ?? Data()
        return String(decoding: data, as: UTF8.self)
    }
}
