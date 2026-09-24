import Foundation
import os

/// Liveness is `GET /healthz` on loopback: the collector's only
/// unauthenticated route. The request carries no credential, and a fresh
/// ephemeral session per probe never uses a proxy, cache or cookie store (the
/// collector closes idle connections, so none is reused either).
public enum LivenessProbe {
    /// Same bound as the runbook's `curl --max-time 3 .../healthz`.
    public static let timeout: TimeInterval = 3

    /// True only if 127.0.0.1:<port> answers as the collector run that wrote
    /// the summary: `{"ok":true,"instanceId":"<that id>"}`. Any other service
    /// on the port, including one answering a bare `{"ok":true}`, is not it.
    public static func answers(as instanceId: String, port: Int) -> Bool {
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

        let verified = OSAllocatedUnfairLock(initialState: false)
        let finished = DispatchSemaphore(value: 0)
        let task = session.dataTask(with: url) { data, response, _ in
            let reply = isCollectorReply(
                statusCode: (response as? HTTPURLResponse)?.statusCode, body: data, instanceId: instanceId
            )
            verified.withLock { $0 = reply }
            finished.signal()
        }
        task.resume()
        if finished.wait(timeout: .now() + timeout) == .timedOut {
            task.cancel()
        }
        return verified.withLock { $0 }
    }

    /// The collector's exact reply: HTTP 200 and a JSON object with exactly
    /// two keys, `ok` (the boolean true) and `instanceId` (the expected id).
    public static func isCollectorReply(statusCode: Int?, body: Data?, instanceId: String) -> Bool {
        guard statusCode == 200, let body, body.count <= 1_024,
              let object = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any],
              Set(object.keys) == ["ok", "instanceId"],
              let reply = try? JSONDecoder().decode(Reply.self, from: body) else {
            return false
        }
        return reply.ok && reply.instanceId == instanceId
    }

    private struct Reply: Decodable {
        let ok: Bool
        let instanceId: String
    }
}
