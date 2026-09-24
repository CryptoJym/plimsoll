/**
 * proof:mac-menubar (#12, eco-6hoxj.163.34): `swift build`, then `swift test`,
 * in packages/mac-menubar. Fails unless both exit 0 and the Swift Testing run
 * reports at least MIN_TESTS passing tests, so a test target that silently
 * ran nothing cannot pass.
 *
 * The tests use Swift Testing, so they need a Swift 6 toolchain: Xcode 16 or
 * newer (CI sets DEVELOPER_DIR), or the Command Line Tools alone. The Command
 * Line Tools ship no XCTest, and SwiftPM then leaves out the Swift Testing
 * framework and interop library paths, so they are passed here.
 */
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "packages", "mac-menubar");
const commandLineTools = "/Library/Developer/CommandLineTools";
const MIN_TESTS = 29;

function run(command: string, args: string[]) {
  const started = performance.now();
  const result = spawnSync(command, args, {
    cwd: packageRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
    maxBuffer: 64 * 1024 * 1024,
  });
  process.stdout.write(result.stdout ?? "");
  const seconds = Number(((performance.now() - started) / 1_000).toFixed(1));
  return { status: result.error ? null : result.status, stdout: result.stdout ?? "", seconds };
}

const swift = spawnSync("xcrun", ["--find", "swift"], { encoding: "utf8" }).stdout?.trim() ?? "";
const testingPaths = swift.startsWith(`${commandLineTools}/`)
  ? [
      "-Xswiftc", "-F", "-Xswiftc", `${commandLineTools}/Library/Developer/Frameworks`,
      "-Xlinker", "-rpath", "-Xlinker", `${commandLineTools}/Library/Developer/Frameworks`,
      "-Xlinker", "-rpath", "-Xlinker", `${commandLineTools}/Library/Developer/usr/lib`,
    ]
  : [];

const build = run("swift", ["build"]);
const test = build.status === 0 ? run("swift", ["test", ...testingPaths]) : null;
// Swift 6.0: "Test run with 29 tests passed"; later: "Test run with 29 tests in 1 suite passed".
const passed = Number(test?.stdout.match(/Test run with (\d+) tests? (?:in \d+ suites? )?passed/)?.[1] ?? 0);
const ok = build.status === 0 && test?.status === 0 && passed >= MIN_TESTS;
console.log(JSON.stringify({
  proof: "mac-menubar",
  ok,
  toolchain: swift.startsWith(`${commandLineTools}/`) ? "command-line-tools" : "xcode",
  build: { exit: build.status, seconds: build.seconds },
  test: test ? { exit: test.status, seconds: test.seconds, passed, minimum: MIN_TESTS } : null,
}));
if (!ok) process.exitCode = 1;
