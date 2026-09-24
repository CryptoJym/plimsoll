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

        // Read both pipes while the collector runs. Waiting for it to exit
        // first deadlocks once the status JSON outgrows the pipe buffer: the
        // collector blocks writing and never exits.
        let stdout = PipeReader(output.fileHandleForReading)
        let stderr = PipeReader(error.fileHandleForReading)
        let deadline = DispatchTime.now() + timeout
        guard exited.wait(timeout: deadline) == .success,
              let standardOutput = stdout.wait(until: deadline),
              let standardError = stderr.wait(until: deadline) else {
            stop(process)
            throw CollectorClientError.timedOut(seconds: Int(timeout.rounded(.up)))
        }
        return CollectorExecutionResult(
            standardOutput: String(decoding: standardOutput, as: UTF8.self),
            standardError: String(decoding: standardError, as: UTF8.self),
            exitCode: process.terminationStatus
        )
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

/// Reads one pipe to end-of-file on a background queue.
private final class PipeReader: @unchecked Sendable {
    private let done = DispatchSemaphore(value: 0)
    // Written once before `done` is signalled; read only after waiting on it.
    private var data = Data()

    init(_ handle: FileHandle) {
        DispatchQueue.global(qos: .utility).async {
            // readToEnd() reports a read error as a Swift error; the older
            // readDataToEndOfFile() raises an exception Swift cannot catch.
            self.data = ((try? handle.readToEnd()) ?? nil) ?? Data()
            self.done.signal()
        }
    }

    func wait(until deadline: DispatchTime) -> Data? {
        done.wait(timeout: deadline) == .success ? data : nil
    }
}
