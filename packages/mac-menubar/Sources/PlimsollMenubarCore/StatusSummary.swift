import Darwin
import Foundation

/// Where the collector keeps its private files: `PLIMSOLL_HOME` when set (the
/// collector requires an absolute path), otherwise its default,
/// ~/Library/Application Support/Plimsoll.
public enum CollectorHome {
    public static func resolve(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        userHome: String = NSHomeDirectory()
    ) -> URL? {
        let custom = environment["PLIMSOLL_HOME"]?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if custom.isEmpty {
            return URL(fileURLWithPath: userHome, isDirectory: true)
                .appendingPathComponent("Library/Application Support/Plimsoll", isDirectory: true)
        }
        return custom.hasPrefix("/") ? URL(fileURLWithPath: custom, isDirectory: true) : nil
    }
}

/// The collector's private status-summary.json. The running daemon rewrites
/// it every 15 s with four lifetime counters from its /status cache, this
/// run's instanceId (also on GET /healthz), its version, port and the write
/// time. Reading it runs no collector command and opens no ledger or
/// credential (collector runbook docs/runbooks/local-status-http.md).
public struct StatusSummary: Equatable, Sendable {
    public static let fileName = "status-summary.json"
    public static let schema = "plimsoll.status-summary/v1"
    /// The collector rewrites the file every 15 s; older than this is stale.
    public static let staleAfter: TimeInterval = 60
    /// A real summary is about 300 bytes.
    public static let maximumBytes = 16_384

    public let instanceId: String
    public let port: Int
    public let updatedAt: Date
    public let eventCount: Int?
    public let tokenAttributedEvents: Int?
    public let totalInputTokens: Int?
    public let totalOutputTokens: Int?

    public var tokenCoveragePercent: Double? {
        guard let eventCount, eventCount > 0, let tokenAttributedEvents else { return nil }
        return Double(tokenAttributedEvents) / Double(eventCount) * 100
    }

    /// Strict: the v1 schema, a v4 UUID instanceId, a TCP port and an ISO-8601
    /// time are required; each counter is a non-negative integer or null.
    public init(json: Data) throws {
        guard json.count <= Self.maximumBytes,
              let wire = try? JSONDecoder().decode(Wire.self, from: json),
              wire.schema == Self.schema,
              Self.isInstanceId(wire.instanceId),
              (1...65_535).contains(wire.port),
              let updatedAt = Self.parseTime(wire.updatedAt) else {
            throw SummaryProblem.unreadable
        }
        let counters = [wire.stats?.count, wire.stats?.tokenAttributedEvents,
                        wire.stats?.totalInputTokens, wire.stats?.totalOutputTokens]
        guard counters.allSatisfy({ ($0 ?? 0) >= 0 }) else { throw SummaryProblem.unreadable }
        instanceId = wire.instanceId
        port = wire.port
        self.updatedAt = updatedAt
        eventCount = counters[0]
        tokenAttributedEvents = counters[1]
        totalInputTokens = counters[2]
        totalOutputTokens = counters[3]
    }

    /// A lowercase v4 UUID, as the collector's `crypto.randomUUID()` produces.
    public static func isInstanceId(_ value: String) -> Bool {
        value.range(
            of: #"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"#,
            options: .regularExpression
        ) != nil
    }

    private static func parseTime(_ text: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.date(from: text)
    }

    private struct Wire: Decodable {
        let schema: String
        let instanceId: String
        let port: Int
        let updatedAt: String
        let stats: Stats?

        struct Stats: Decodable {
            let count: Int?
            let tokenAttributedEvents: Int?
            let totalInputTokens: Int?
            let totalOutputTokens: Int?
        }
    }
}

/// Why there is no summary to show. Each has one fixed message; nothing read
/// from disk or the environment is ever displayed.
public enum SummaryProblem: Error, Equatable, Sendable {
    /// No file: the collector has not run since it began writing one.
    case missing
    /// A symlink, not a regular file, another user's, or open to group/other.
    case notPrivate
    /// Too large, unreadable, or not a valid summary.
    case unreadable
    /// PLIMSOLL_HOME is set but not an absolute path.
    case invalidHome

    public var message: String {
        switch self {
        case .missing: return "No status summary found"
        case .notPrivate: return "Status summary is not a private file"
        case .unreadable: return "Status summary unreadable"
        case .invalidHome: return "PLIMSOLL_HOME is not an absolute path"
        }
    }
}

public enum SummaryFile {
    /// Opens without following a symlink (or blocking on a FIFO), checks the
    /// open file is the user's own private regular file, and reads at most
    /// `StatusSummary.maximumBytes`.
    public static func read(home: URL?) -> Result<StatusSummary, SummaryProblem> {
        guard let home else { return .failure(.invalidHome) }
        let path = home.appendingPathComponent(StatusSummary.fileName).path
        let descriptor = open(path, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
        guard descriptor >= 0 else {
            switch errno {
            case ENOENT: return .failure(.missing)
            case ELOOP: return .failure(.notPrivate)
            default: return .failure(.unreadable)
            }
        }
        defer { close(descriptor) }
        var info = stat()
        guard fstat(descriptor, &info) == 0 else { return .failure(.unreadable) }
        guard info.st_mode & S_IFMT == S_IFREG, info.st_uid == getuid(), info.st_mode & 0o077 == 0 else {
            return .failure(.notPrivate)
        }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while data.count <= StatusSummary.maximumBytes {
            let count = Darwin.read(descriptor, &buffer, buffer.count)
            if count == 0 { break }
            if count < 0 {
                if errno == EINTR { continue }
                return .failure(.unreadable)
            }
            data.append(contentsOf: buffer[..<count])
        }
        guard let summary = try? StatusSummary(json: data) else { return .failure(.unreadable) }
        return .success(summary)
    }
}
