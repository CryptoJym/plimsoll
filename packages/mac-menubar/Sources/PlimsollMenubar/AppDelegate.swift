import AppKit
import Foundation
import PlimsollMenubarCore

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate, NSMenuDelegate {
    private let home = CollectorHome.resolve()
    private var statusItem: NSStatusItem?
    private let statusItemTitle = NSMenuItem()
    private let tokenItemTitle = NSMenuItem()
    private let permissionItem = NSMenuItem()
    private var dashboardItem: NSMenuItem?
    private var dashboardURL: URL?
    private var refreshInFlight = false

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
        // Items are enabled explicitly: Open Dashboard only while running.
        menu.autoenablesItems = false
        statusItemTitle.title = "Plimsoll — loading…"
        statusItemTitle.isEnabled = false
        menu.addItem(statusItemTitle)
        tokenItemTitle.title = "Tokens: —"
        tokenItemTitle.isEnabled = false
        menu.addItem(tokenItemTitle)
        menu.addItem(.separator())
        menu.addItem(menuItem("Refresh", action: #selector(refreshAction)))
        let dashboard = menuItem("Open Dashboard", action: #selector(openDashboardAction))
        dashboard.isEnabled = false
        menu.addItem(dashboard)
        dashboardItem = dashboard
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
        guard let dashboardURL else { return }
        NSWorkspace.shared.open(dashboardURL)
    }

    @objc private func quitAction() {
        NSApplication.shared.terminate(nil)
    }

    /// Reads the summary file and checks liveness off the main thread; a
    /// refresh already in flight absorbs further requests.
    private func refreshStatus() {
        guard !refreshInFlight else { return }
        refreshInFlight = true
        let home = self.home
        DispatchQueue.global(qos: .utility).async {
            let lines = StatusLines(state: CollectorMonitor.state(home: home))
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.refreshInFlight = false
                self.render(lines)
            }
        }
    }

    private func render(_ lines: StatusLines) {
        statusItemTitle.title = lines.summary
        tokenItemTitle.title = lines.tokens
        dashboardURL = lines.dashboard
        dashboardItem?.isEnabled = lines.dashboard != nil
    }
}
