import Foundation
import os

public struct CollectorExecutionResult: Equatable, Sendable {
    public let standardOutput: String
    public let standardError: String
    public let exitCode: Int32

    public init(standardOutput: String, standardError: String, exitCode: Int32) {
        self.standardOutput = standardOutput
        self.standardError = standardError
        self.exitCode = exitCode
    }
}

public enum CollectorClientError: Error, Equatable, LocalizedError {
    case noCollectorConfigured
    case commandFailed(exitCode: Int32, message: String)
    case invalidStatusOutput
    case processLaunchFailed(String)
    case timedOut(seconds: Int)

    public var errorDescription: String? {
        switch self {
        case .noCollectorConfigured:
            return "Configure PLIMSOLL_COLLECTOR_BIN or PLIMSOLL_COLLECTOR_REPO."
        case let .commandFailed(exitCode, message):
            return "Collector exited with status \(exitCode): \(message)"
        case .invalidStatusOutput:
            return "Collector returned invalid status JSON."
        case let .processLaunchFailed(message):
            return "Could not launch collector: \(message)"
        case let .timedOut(seconds):
            return "Collector status did not finish within \(seconds) seconds."
        }
    }
}

/// Reads collector state. It runs `status` and probes `/healthz`; it has no
/// operation that starts, stops or changes the collector.
public final class CollectorClient: @unchecked Sendable {
    public typealias Execute = (CollectorInvocation) throws -> CollectorExecutionResult
    public typealias ProbeLiveness = (Int) -> Bool

    private let invocation: CollectorInvocation
    private let execute: Execute
    private let probeLiveness: ProbeLiveness

    public init(
        invocation: CollectorInvocation,
        execute: @escaping Execute = ProcessCollectorExecutor.run,
        probeLiveness: @escaping ProbeLiveness = CollectorClient.defaultProbeLiveness
    ) {
        self.invocation = invocation
        self.execute = execute
        self.probeLiveness = probeLiveness
    }

    public func status() throws -> CollectorStatus {
        let result = try execute(invocation)
        guard result.exitCode == 0 else {
            throw CollectorClientError.commandFailed(
                exitCode: result.exitCode,
                message: CollectorMessage.displayLine(result.standardError)
            )
        }
        guard let data = result.standardOutput.data(using: .utf8) else {
            throw CollectorClientError.invalidStatusOutput
        }
        do {
            return try CollectorStatus(json: data)
        } catch {
            throw CollectorClientError.invalidStatusOutput
        }
    }

    public func snapshot() throws -> CollectorSnapshot {
        let status = try status()
        return CollectorSnapshot(running: probeLiveness(status.port), status: status)
    }

    /// Same bound as the runbook's `curl --max-time 3 .../healthz`.
    public static let livenessTimeout: TimeInterval = 3

    /// Liveness is `GET /healthz` on loopback: the collector's only
    /// unauthenticated route. The request carries no credential, and a fresh
    /// ephemeral session per probe never uses a proxy, cache or cookie store
    /// (the collector closes idle connections, so none is reused either).
    public static func defaultProbeLiveness(port: Int) -> Bool {
        guard (1...65_535).contains(port),
              let url = URL(string: "http://127.0.0.1:\(port)/healthz") else { return false }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.connectionProxyDictionary = [:]
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        configuration.urlCredentialStorage = nil
        configuration.timeoutIntervalForRequest = livenessTimeout
        configuration.timeoutIntervalForResource = livenessTimeout
        let session = URLSession(configuration: configuration)
        defer { session.invalidateAndCancel() }

        let healthy = OSAllocatedUnfairLock(initialState: false)
        let finished = DispatchSemaphore(value: 0)
        let task = session.dataTask(with: url) { data, response, _ in
            let reply = isHealthzReply(statusCode: (response as? HTTPURLResponse)?.statusCode, body: data)
            healthy.withLock { $0 = reply }
            finished.signal()
        }
        task.resume()
        if finished.wait(timeout: .now() + livenessTimeout) == .timedOut {
            task.cancel()
        }
        return healthy.withLock { $0 }
    }

    /// The collector answers `/healthz` with HTTP 200 and `{"ok":true}`.
    /// Anything else on that port is not a live collector.
    public static func isHealthzReply(statusCode: Int?, body: Data?) -> Bool {
        guard statusCode == 200, let body,
              let reply = try? JSONDecoder().decode(HealthzReply.self, from: body) else {
            return false
        }
        return reply.ok
    }

    private struct HealthzReply: Decodable {
        let ok: Bool
    }
}

public enum ProcessCollectorExecutor {
    /// Bound on one `plimsoll status` run. Status opens and reads the local
    /// ledger, which takes seconds on a busy host, not minutes.
    public static let defaultTimeout: TimeInterval = 60

    public static func run(_ invocation: CollectorInvocation) throws -> CollectorExecutionResult {
        try run(invocation, timeout: defaultTimeout)
    }

    public static func run(
        _ invocation: CollectorInvocation,
        timeout: TimeInterval
    ) throws -> CollectorExecutionResult {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: invocation.executablePath)
        process.arguments = invocation.arguments
        process.standardInput = FileHandle.nullDevice

        let output = Pipe()
        let error = Pipe()
        process.standardOutput = output
        process.standardError = error
        let exited = DispatchSemaphore(value: 0)
        process.terminationHandler = { _ in exited.signal() }

        do {
            try process.run()
        } catch {
            throw CollectorClientError.processLaunchFailed(error.localizedDescription)
        }

        // Read both pipes on this thread while the collector runs. Waiting for
        // it to exit first deadlocks once the status JSON outgrows the pipe
        // buffer. Handing the reads to DispatchQueue.global() stalls whenever
        // every Swift concurrency pool thread is blocked: Dispatch then starts
        // no thread for global-queue work, so the reads never begin.
        let deadline = Date().addingTimeInterval(timeout)
        let descriptors = [output.fileHandleForReading.fileDescriptor, error.fileHandleForReading.fileDescriptor]
        guard let streams = readToEnd(descriptors, until: deadline),
              exited.wait(timeout: .now() + max(0, deadline.timeIntervalSinceNow)) == .success else {
            stop(process)
            throw CollectorClientError.timedOut(seconds: Int(timeout.rounded(.up)))
        }
        return CollectorExecutionResult(
            standardOutput: String(decoding: streams[0], as: UTF8.self),
            standardError: String(decoding: streams[1], as: UTF8.self),
            exitCode: process.terminationStatus
        )
    }

    /// Reads each descriptor to end-of-file with poll(2), or returns nil if the
    /// deadline passes first.
    private static func readToEnd(_ descriptors: [Int32], until deadline: Date) -> [Data]? {
        var streams = descriptors.map { _ in Data() }
        var watched = descriptors.map { pollfd(fd: $0, events: Int16(POLLIN), revents: 0) }
        var buffer = [UInt8](repeating: 0, count: 65_536)
        while watched.contains(where: { $0.fd >= 0 }) {
            let remaining = deadline.timeIntervalSinceNow
            guard remaining > 0 else { return nil }
            let ready = poll(&watched, nfds_t(watched.count), Int32((min(remaining, 60) * 1000).rounded(.up)))
            if ready < 0 {
                if errno == EINTR { continue }
                return nil
            }
            for index in watched.indices where watched[index].fd >= 0 && watched[index].revents != 0 {
                let count = read(watched[index].fd, &buffer, buffer.count)
                if count > 0 {
                    streams[index].append(contentsOf: buffer[..<count])
                } else if count == 0 || errno != EINTR {
                    watched[index].fd = -1 // end of file, or a read error: stop watching it
                }
            }
        }
        return streams
    }

    private static func stop(_ process: Process) {
        guard process.isRunning else { return }
        process.terminate()
        let deadline = Date().addingTimeInterval(2)
        while process.isRunning, Date() < deadline {
            usleep(10_000)
        }
        if process.isRunning {
            kill(process.processIdentifier, SIGKILL)
        }
    }
}
