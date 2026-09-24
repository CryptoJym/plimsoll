import AppKit
import Foundation
import PlimsollMenubarCore

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate, NSMenuDelegate {
    private let client: CollectorClient?
    private var statusItem: NSStatusItem?
    private let statusItemTitle = NSMenuItem()
    private let tokenItemTitle = NSMenuItem()
    private let permissionItem = NSMenuItem()
    private var dashboardPort = 48271
    private var refreshInFlight = false

    override init() {
        if let invocation = CollectorInvocation(environment: ProcessInfo.processInfo.environment) {
            self.client = CollectorClient(invocation: invocation)
        } else {
            self.client = nil
        }
        super.init()
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        configureStatusItem()
        refreshStatus()
    }

    /// Opening the menu re-reads status, so what it shows is current.
    func menuWillOpen(_ menu: NSMenu) {
        refreshStatus()
    }

    private func configureStatusItem() {
        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        item.button?.title = "PL"
        item.button?.toolTip = "Plimsoll collector"

        let menu = NSMenu()
        menu.delegate = self
        statusItemTitle.title = "Plimsoll — loading…"
        statusItemTitle.isEnabled = false
        menu.addItem(statusItemTitle)
        tokenItemTitle.title = "Tokens: —"
        tokenItemTitle.isEnabled = false
        menu.addItem(tokenItemTitle)
        menu.addItem(.separator())
        menu.addItem(menuItem("Refresh", action: #selector(refreshAction)))
        menu.addItem(menuItem("Open Dashboard", action: #selector(openDashboardAction)))
        menu.addItem(.separator())
        permissionItem.title = "Permission doctor: no additional permissions"
        permissionItem.isEnabled = false
        menu.addItem(permissionItem)
        menu.addItem(.separator())
        menu.addItem(menuItem("Quit Plimsoll Menubar", action: #selector(quitAction)))
        item.menu = menu
        statusItem = item
    }

    private func menuItem(_ title: String, action: Selector) -> NSMenuItem {
        let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
        item.target = self
        return item
    }

    @objc private func refreshAction() {
        refreshStatus()
    }

    @objc private func openDashboardAction() {
        guard let url = URL(string: "http://127.0.0.1:\(dashboardPort)") else { return }
        NSWorkspace.shared.open(url)
    }

    @objc private func quitAction() {
        NSApplication.shared.terminate(nil)
    }

    /// Runs one `status` read off the main thread; a refresh already in
    /// flight absorbs further requests instead of stacking collector runs.
    private func refreshStatus() {
        guard let client else {
            render(StatusLines(error: CollectorClientError.noCollectorConfigured))
            return
        }
        guard !refreshInFlight else { return }
        refreshInFlight = true
        DispatchQueue.global(qos: .utility).async {
            let result = Result { try client.snapshot() }
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.refreshInFlight = false
                switch result {
                case let .success(snapshot):
                    self.dashboardPort = snapshot.port
                    self.render(StatusLines(snapshot: snapshot))
                case let .failure(error):
                    self.render(StatusLines(error: error))
                }
            }
        }
    }

    private func render(_ lines: StatusLines) {
        statusItemTitle.title = lines.summary
        tokenItemTitle.title = lines.tokens
    }
}
