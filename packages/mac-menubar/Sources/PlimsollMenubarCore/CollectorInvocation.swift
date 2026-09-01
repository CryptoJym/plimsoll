import Foundation

public enum CollectorCommand: String, CaseIterable, Sendable {
    case status
    case start
    case stop
}

/// A shell-free invocation of the collector CLI.
public struct CollectorInvocation: Equatable, Sendable {
    public let executablePath: String
    public let arguments: [String]
    public let command: CollectorCommand

    public init?(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        command: CollectorCommand = .status
    ) {
        if let binary = Self.nonEmpty(environment["PLIMSOLL_COLLECTOR_BIN"]) {
            self.executablePath = binary
            self.arguments = [command.rawValue]
        } else if let repository = Self.nonEmpty(environment["PLIMSOLL_COLLECTOR_REPO"]) {
            let pnpm = Self.nonEmpty(environment["PLIMSOLL_PNPM_BIN"])
            self.executablePath = pnpm ?? "/usr/bin/env"
            self.arguments = pnpm == nil
                ? ["pnpm", "--dir", repository, "collector", command.rawValue]
                : ["--dir", repository, "collector", command.rawValue]
        } else {
            return nil
        }
        self.command = command
    }

    public func replacing(command: CollectorCommand) -> CollectorInvocation {
        // This initializer is intentionally private to keep all externally
        // supplied invocations on one of the two documented paths above.
        CollectorInvocation(
            executablePath: executablePath,
            arguments: argumentsFor(command: command),
            command: command
        )
    }

    private init(executablePath: String, arguments: [String], command: CollectorCommand) {
        self.executablePath = executablePath
        self.arguments = arguments
        self.command = command
    }

    private func argumentsFor(command: CollectorCommand) -> [String] {
        if arguments == [self.command.rawValue] { return [command.rawValue] }
        return Array(arguments.dropLast()) + [command.rawValue]
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let value, !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }
        return value
    }
}
