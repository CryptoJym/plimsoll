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
