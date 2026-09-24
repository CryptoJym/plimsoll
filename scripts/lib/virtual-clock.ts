/**
 * A virtual performance clock for scheduling proofs (eco-6hoxj.163.42).
 *
 * Automatic maintenance bounds its work with `performance.now()`. A proof
 * that models slow work with real sleeps measures the host as much as the
 * code: on a loaded machine the same fixture admits less work, and a count
 * that depends on it drifts. With this clock installed, real work costs no
 * time; only what the proof charges with `spend()` advances it. Every
 * admission decision then follows from the fixture alone, and a run gives
 * the same result on any machine (docs/architecture/resource-budget-gates.md:
 * window-sensitive assertions use an injected clock, not the wall clock).
 *
 * `Date.now()` is left alone: file times and event timestamps stay real.
 */
const realNow = performance.now.bind(performance);
let virtualMs = 0;
let installed = false;

export function installVirtualClock(startMs = 1_000) {
  virtualMs = startMs;
  installed = true;
  performance.now = () => virtualMs;
}

/** Charge `ms` of virtual time, as a slow synchronous call would take it. */
export function spend(ms: number) {
  if (!installed) throw new Error("virtual_clock_not_installed");
  virtualMs += Math.max(0, ms);
}

export function virtualNow() {
  return virtualMs;
}

export function restoreRealClock() {
  performance.now = realNow;
  installed = false;
}
