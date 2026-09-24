import Foundation

/// A shell-free invocation of the collector's `status` command.
///
/// `status` is the only collector command the menubar can build: the app is
/// read-only and never starts, stops or reconfigures the collector.
public struct CollectorInvocation: Equatable, Sendable {
    public let executablePath: String
    public let arguments: [String]

    public init?(environment: [String: String] = ProcessInfo.processInfo.environment) {
        if let binary = Self.nonEmpty(environment["PLIMSOLL_COLLECTOR_BIN"]) {
            self.executablePath = binary
            self.arguments = ["status"]
        } else if let repository = Self.nonEmpty(environment["PLIMSOLL_COLLECTOR_REPO"]) {
            // --silent keeps pnpm's "> @plimsoll/monorepo collector" banner
            // off stdout, which must be the status JSON alone.
            let pnpmArguments = ["--silent", "--dir", repository, "collector", "status"]
            let pnpm = Self.nonEmpty(environment["PLIMSOLL_PNPM_BIN"])
            self.executablePath = pnpm ?? "/usr/bin/env"
            self.arguments = pnpm == nil ? ["pnpm"] + pnpmArguments : pnpmArguments
        } else {
            return nil
        }
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let value, !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }
        return value
    }
}
