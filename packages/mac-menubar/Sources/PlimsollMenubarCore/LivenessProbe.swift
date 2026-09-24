import Foundation
import os

/// Liveness is `GET /healthz` on loopback: the collector's only
/// unauthenticated route. The request carries no credential, and a fresh
/// ephemeral session per probe never uses a proxy, cache or cookie store (the
/// collector closes idle connections, so none is reused either).
public enum LivenessProbe {
    /// Same bound as the runbook's `curl --max-time 3 .../healthz`.
    public static let timeout: TimeInterval = 3

    public static func healthz(port: Int) -> Bool {
        guard (1...65_535).contains(port),
              let url = URL(string: "http://127.0.0.1:\(port)/healthz") else { return false }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.connectionProxyDictionary = [:]
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        configuration.urlCredentialStorage = nil
        configuration.timeoutIntervalForRequest = timeout
        configuration.timeoutIntervalForResource = timeout
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
        if finished.wait(timeout: .now() + timeout) == .timedOut {
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
