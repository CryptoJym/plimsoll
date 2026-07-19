# LaunchAgent manifest transaction

The user LaunchAgent is an owned local control-plane object, not a generic
plist merge target. Installation accepts only `com.plimsoll.collector` and an
exact semantic schema: one supported development or packaged runtime, one
working directory, crash-only restart policy, private log paths, and exactly
the `PATH` plus metadata-only environment keys. XML entities, aliases,
duplicate or confusable keys, extra arguments, extra environment, relative
runtime paths, and unrelated manifests fail closed.

Preview inspects the existing path without creating a directory, config,
manifest, backup, or process. Apply validates the home and every existing
ancestor with no-follow identity checks, creates missing user directories as
`0700`, and rejects writable, foreign-owned, linked, symlinked, or non-regular
objects. The rendered plist is written `0600` to a same-directory exclusive
file, fsynced, read back through its bound descriptor, and parsed again before
publication. `0600` is an exact four-octal-digit postcondition: setuid,
setgid, and sticky bits are rejected rather than hidden by a `0777` mask.

Node on macOS does not expose a compare-and-swap rename. Replacement therefore
uses a no-clobber protocol: persist an exact private preimage and rollback
receipt; atomically claim the old inode by rename; verify that claim; atomically
rename the prepared inode to a private commit name; and publish the same inode
with an exclusive hard link. A concurrent destination wins instead of being
overwritten. The parent directory is fsynced, then the visible inode, bytes,
mode, ownership, and plist semantics are re-read before success. Any detected
ancestor, leaf, content, hardlink, or post-publication change returns a literal
failure and preserves the competing object.

Loading is a later gate. The CLI binds the exact visible manifest digest and
filesystem identity before bootstrap, then reopens and compares both after a
successful `launchctl bootstrap`. A changed or invalid post-bootstrap object
returns `post_bootstrap_manifest_changed` with `loaded: false`, attempts a
label-scoped `bootout`, and reports the bootout result plus the subsequent
`launchctl print` exit/error and whether the label was reported. A failed
bootstrap reports `launchctl_failed`; a repeated load checks the existing
label and does not bootstrap a second
owner. Uninstall accepts only the strict owned manifest, moves it to a private
claim, validates the claim, fsyncs visible absence, and never removes an
unrelated plist. Runtime identity, PID, listener, and fresh-signal binding
remain the doctor/canary gates rather than manifest-install claims.
