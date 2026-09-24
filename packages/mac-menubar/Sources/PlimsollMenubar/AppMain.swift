import AppKit
import PlimsollMenubarCore

@main
enum PlimsollMenubarMain {
    @MainActor
    static func main() {
        if CommandLine.arguments.contains("--doctor") {
            do {
                print(try PermissionDoctor.json())
            } catch {
                fputs("permission doctor failed: \(error)\n", stderr)
                exit(1)
            }
            return
        }

        // Prints the menu's two status lines once, without starting the app.
        if CommandLine.arguments.contains("--status") {
            guard let invocation = CollectorInvocation() else {
                print(StatusLines(error: CollectorClientError.noCollectorConfigured).json())
                exit(1)
            }
            do {
                print(StatusLines(snapshot: try CollectorClient(invocation: invocation).snapshot()).json())
            } catch {
                print(StatusLines(error: error).json())
                exit(1)
            }
            return
        }

        let application = NSApplication.shared
        let delegate = AppDelegate()
        application.delegate = delegate
        application.setActivationPolicy(.accessory)
        application.run()
    }
}
