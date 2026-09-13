import AppKit
import Foundation
import PlimsollMenubarCore

final class AppDelegate: NSObject, NSApplicationDelegate {
    private let client: CollectorClient?
    private var statusItem: NSStatusItem?
    private let statusItemTitle = NSMenuItem()
    private let tokenItemTitle = NSMenuItem()
    private let permissionItem = NSMenuItem()
    private var dashboardPort = 48271

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

    private func configureStatusItem() {
        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        item.button?.title = "PL"
        item.button?.toolTip = "Plimsoll collector"

        let menu = NSMenu()
        statusItemTitle.title = "Plimsoll — loading…"
        statusItemTitle.isEnabled = false
        menu.addItem(statusItemTitle)
        tokenItemTitle.title = "Tokens: —"
        tokenItemTitle.isEnabled = false
        menu.addItem(tokenItemTitle)
        menu.addItem(.separator())
        menu.addItem(menuItem("Refresh", action: #selector(refreshAction)))
        menu.addItem(menuItem("Start Collector", action: #selector(startAction)))
        menu.addItem(menuItem("Stop Collector", action: #selector(stopAction)))
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

    @objc private func startAction() {
        runCommand { try self.client?.start() }
    }

    @objc private func stopAction() {
        runCommand { try self.client?.stop() }
    }

    @objc private func openDashboardAction() {
        guard let url = URL(string: "http://127.0.0.1:\(dashboardPort)") else { return }
        NSWorkspace.shared.open(url)
    }

    @objc private func quitAction() {
        NSApplication.shared.terminate(nil)
    }

    private func runCommand(_ operation: @escaping () throws -> CollectorExecutionResult?) {
        guard client != nil else {
            render(error: CollectorClientError.noCollectorConfigured)
            return
        }
        DispatchQueue.global(qos: .utility).async { [weak self] in
            do {
                _ = try operation()
                self?.refreshStatus()
            } catch {
                DispatchQueue.main.async { self?.render(error: error) }
            }
        }
    }

    private func refreshStatus() {
        guard client != nil else {
            render(error: CollectorClientError.noCollectorConfigured)
            return
        }
        DispatchQueue.global(qos: .utility).async { [weak self] in
            guard let self else { return }
            do {
                let snapshot = try self.client!.snapshot()
                DispatchQueue.main.async { self.render(snapshot: snapshot) }
            } catch {
                DispatchQueue.main.async { self.render(error: error) }
            }
        }
    }

    private func render(snapshot: CollectorSnapshot) {
        dashboardPort = snapshot.port
        let state = snapshot.running ? "Running" : "Stopped"
        let count = snapshot.eventCount.map(String.init) ?? "—"
        let coverage = snapshot.tokenCoveragePercent.map { String(format: "%.1f%%", $0) } ?? "—"
        statusItemTitle.title = "\(state) · \(count) events · \(coverage) token coverage"

        let input = snapshot.totalInputTokens.map(String.init) ?? "—"
        let output = snapshot.totalOutputTokens.map(String.init) ?? "—"
        tokenItemTitle.title = "Tokens: \(input) in · \(output) out"
    }

    private func render(error: Error) {
        statusItemTitle.title = "Collector unavailable"
        tokenItemTitle.title = error.localizedDescription
    }
}
