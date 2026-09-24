import Foundation

/// Collector error text the menu may show. The app never reads the
/// collector's credential file; `plimsoll status` presents the management
/// credential to its own daemon. Its stderr is shown only as one bounded line,
/// with anything shaped like a Plimsoll credential (43 base64url characters)
/// replaced, so a future error message could not put a token on screen.
enum CollectorMessage {
    static let maximumLength = 200

    private static let credentialShape = #"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{43}(?![A-Za-z0-9_-])"#

    static func displayLine(_ text: String) -> String {
        let redacted = text.replacingOccurrences(
            of: credentialShape,
            with: "[redacted]",
            options: .regularExpression
        )
        let line = redacted
            .split(whereSeparator: \.isNewline)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .first { !$0.isEmpty } ?? ""
        if line.isEmpty { return "no error output" }
        return line.count > maximumLength ? String(line.prefix(maximumLength)) + "…" : line
    }
}
