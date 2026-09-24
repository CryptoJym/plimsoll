import Foundation

public struct PermissionDoctorReport: Codable, Equatable, Sendable {
    public let accessibility: Bool
    public let camera: Bool
    public let inputMonitoring: Bool
    public let microphone: Bool
    public let screenRecording: Bool

    public var requestsAdditionalPermissions: Bool {
        accessibility || camera || inputMonitoring || microphone || screenRecording
    }

    public var summary: String {
        requestsAdditionalPermissions
            ? "Additional macOS permissions requested"
            : "No additional macOS permissions requested"
    }
}

public enum PermissionDoctor {
    public static func report() -> PermissionDoctorReport {
        PermissionDoctorReport(
            accessibility: false,
            camera: false,
            inputMonitoring: false,
            microphone: false,
            screenRecording: false
        )
    }

    public static func json() throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let current = report()
        let data = try encoder.encode(DoctorOutput(
            accessibility: current.accessibility,
            camera: current.camera,
            inputMonitoring: current.inputMonitoring,
            microphone: current.microphone,
            screenRecording: current.screenRecording,
            requestsAdditionalPermissions: current.requestsAdditionalPermissions,
            summary: current.summary
        ))
        return String(decoding: data, as: UTF8.self)
    }

    private struct DoctorOutput: Encodable {
        let accessibility: Bool
        let camera: Bool
        let inputMonitoring: Bool
        let microphone: Bool
        let screenRecording: Bool
        let requestsAdditionalPermissions: Bool
        let summary: String
    }
}
