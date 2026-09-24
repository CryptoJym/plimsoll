import CryptoKit
import Foundation
import os

/// Liveness is `GET /healthz?challenge=` on loopback: the collector's only
/// unauthenticated route. The request carries a fresh random challenge and no
/// credential, and a fresh ephemeral session per probe never uses a proxy,
/// cache or cookie store (the collector closes idle connections, so none is
/// reused either).
public enum LivenessProbe {
    /// Same bound as the runbook's `curl --max-time 3 .../healthz`.
    public static let timeout: TimeInterval = 3
    /// Names what the proof authenticates (the collector's HEALTHZ_PROOF_CONTEXT).
    static let proofContext = "plimsoll.healthz-proof/v1"

    /// True only if 127.0.0.1:<port> proves it is the collector run that wrote
    /// the summary: it answers a fresh random challenge with the HMAC that only
    /// a holder of the summary's key can compute. Any other service on the
    /// port, including one replaying the public instanceId or an earlier
    /// answer, is not it.
    public static func answers(for summary: StatusSummary) -> Bool {
        answers(port: summary.port, instanceId: summary.instanceId, key: summary.healthzKey)
    }

    static func answers(port: Int, instanceId: String, key: Data) -> Bool {
        let challenge = newChallenge()
        guard (1...65_535).contains(port),
              let url = URL(string: "http://127.0.0.1:\(port)/healthz?challenge=\(challenge)") else { return false }
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
                statusCode: (response as? HTTPURLResponse)?.statusCode, body: data,
                port: port, instanceId: instanceId, key: key, challenge: challenge
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

    /// 32 random bytes as unpadded base64url, new for every check, so no
    /// earlier answer can be replayed.
    static func newChallenge() -> String {
        Base64URL.encode(SymmetricKey(size: .bits256).withUnsafeBytes { Data($0) })
    }

    /// The collector's exact reply to `challenge`: HTTP 200 and a JSON object
    /// with exactly `ok` (the boolean true), `instanceId` (the summary's) and
    /// `proof`, the base64url HMAC-SHA256 under the summary's key of the
    /// context, the port, the instanceId and the challenge, one per line,
    /// verified in constant time.
    public static func isCollectorReply(statusCode: Int?, body: Data?, summary: StatusSummary, challenge: String) -> Bool {
        isCollectorReply(statusCode: statusCode, body: body, port: summary.port, instanceId: summary.instanceId,
                         key: summary.healthzKey, challenge: challenge)
    }

    static func isCollectorReply(
        statusCode: Int?, body: Data?, port: Int, instanceId: String, key: Data, challenge: String
    ) -> Bool {
        guard statusCode == 200, let body, body.count <= 1_024,
              let object = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any],
              Set(object.keys) == ["ok", "instanceId", "proof"],
              let reply = try? JSONDecoder().decode(Reply.self, from: body),
              reply.ok, reply.instanceId == instanceId,
              let proof = Base64URL.decode32(reply.proof) else {
            return false
        }
        let message = Data("\(proofContext)\n\(port)\n\(instanceId)\n\(challenge)".utf8)
        return HMAC<SHA256>.isValidAuthenticationCode(proof, authenticating: message, using: SymmetricKey(data: key))
    }

    private struct Reply: Decodable {
        let ok: Bool
        let instanceId: String
        let proof: String
    }
}
