import Foundation

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
            let message = result.standardError.trimmingCharacters(in: .whitespacesAndNewlines)
            throw CollectorClientError.commandFailed(
                exitCode: result.exitCode,
                message: message.isEmpty ? "no error output" : message
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

    public static func defaultProbeLiveness(port: Int) -> Bool {
        guard let url = URL(string: "http://127.0.0.1:\(port)/healthz") else { return false }
        let semaphore = DispatchSemaphore(value: 0)
        var isHealthy = false
        let task = URLSession.shared.dataTask(with: url) { _, response, _ in
            isHealthy = (response as? HTTPURLResponse)?.statusCode == 200
            semaphore.signal()
        }
        task.resume()
        _ = semaphore.wait(timeout: .now() + 1.0)
        task.cancel()
        return isHealthy
    }
}

public enum ProcessCollectorExecutor {
    public static func run(_ invocation: CollectorInvocation) throws -> CollectorExecutionResult {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: invocation.executablePath)
        process.arguments = invocation.arguments

        let output = Pipe()
        let error = Pipe()
        process.standardOutput = output
        process.standardError = error

        do {
            try process.run()
        } catch {
            throw CollectorClientError.processLaunchFailed(error.localizedDescription)
        }
        process.waitUntilExit()

        let stdout = String(data: output.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
        let stderr = String(data: error.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
        return CollectorExecutionResult(
            standardOutput: stdout,
            standardError: stderr,
            exitCode: process.terminationStatus
        )
    }
}
