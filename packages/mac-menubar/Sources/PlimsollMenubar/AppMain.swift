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
                fputs("permission doctor failed\n", stderr)
                exit(1)
            }
            return
        }

        // Prints the menu's status lines once, without starting the app.
        if CommandLine.arguments.contains("--status") {
            let state = CollectorMonitor.state(home: CollectorHome.resolve())
            print(StatusLines(state: state).json())
            if case .unavailable = state { exit(1) }
            return
        }

        let application = NSApplication.shared
        let delegate = AppDelegate()
        application.delegate = delegate
        application.setActivationPolicy(.accessory)
        application.run()
    }
}
