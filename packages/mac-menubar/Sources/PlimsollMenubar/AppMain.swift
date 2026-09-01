import AppKit
import PlimsollMenubarCore

@main
enum PlimsollMenubarMain {
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

        let application = NSApplication.shared
        let delegate = AppDelegate()
        application.delegate = delegate
        application.setActivationPolicy(.accessory)
        application.run()
    }
}
