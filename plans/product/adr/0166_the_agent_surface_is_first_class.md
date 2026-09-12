# ADR 0166: the agent surface is first class

Status: proposed 2026-09-12. Application lane. Polariton was started for
people; PRODUCT-0158, 0160, and 0162 then gave agents a batch driver, a live
same-user connection, and notebook actions on Linux. ADR 0163 made the
desktop native without carrying that surface along, and ADR 0165's
acceptance found the gap the hard way: on Windows there is no way for an
agent to reach a running Polariton, and on no platform can an agent connect
a database. Driving the window with synthetic keystrokes was tried in that
gap and must never be tried again: it depends on keyboard focus, it cannot
verify what it touched, and it put field values into a text editor and
launched unrelated applications.

## Decision

### 1. Every supported user operation has an agent equivalent

An operation a person can perform in Polariton is reachable by an agent
through the product's own command surface, with a receipt, on every
desktop Polariton ships on. This is the invariant; each cut delivers a
bounded slice of it and names what remains.

### 2. One application operation serves the form and the command

The GUI form and the agent command enter the same application operation:
the same validation, the same state transitions, the same admission. A
receipt reports the operation's completed outcome, success or failure with
the reason the application knows, never that it was merely requested.

### 3. Two generations, each named

Two lifetimes are in play and a command names both. The live-connection
generation identifies one agent's session with a window and already
governs request ids and gate closure; it is unchanged here. The database
connection generation is minted by the adapter owner on every transition
of the external connection: each connect that completes, each disconnect,
each replacement. `connect` returns the generation it established;
`disconnect` and `connection_status` name the generation they act on, and
a request naming any other generation is refused as stale, not applied.
The window's person is never displaced: a connect result that arrives
after the person has chosen another connection does not install itself,
and a disconnect aimed at a superseded generation cannot tear down its
replacement. No command depends on keyboard focus, pointer position, or
the timing of a menu animation.

### 4. The same-user boundary holds on every platform, both ways

The live connection admits only the same user, as the Unix socket does
today, and the client establishes that it reached the advertised Polariton
process, not merely a server that admits the right user. On Windows the
transport is a named pipe whose DACL admits the running user alone; the
client checks the pipe server's process against the advertisement's pid
and process-start token before sending anything, and the server checks the
client's user on accept. The advertisement directory's ownership and
non-symlink checks are their own contract, distinct from the private-key
guard, even where the low-level security calls are shared. A loopback TCP
transport with a token is not adopted without its own security design.

### 5. Shared behavior is tested, not just shared names

An inventory that pairs menu actions with commands catches omissions; it
does not prove the two paths do the same thing. For each operation this
cut adds, a test drives the form path and the command path through the
same application owner and asserts the same state and the same receipt.

## This cut

Windows live access: the advertised connection, `windows`, `observe`, and
`act` on Windows over a named pipe under §4. Database connection commands
in both the batch driver and the live vocabulary: `connect` taking the
adapter selection and the same fields as the form, `disconnect`, and
`connection_status`, each through the adapter owner the form already uses
and each carrying the database connection generation of §3. The Rune,
plot, and publication actions already have commands. Everything else a
person can do that still has no command is listed in the implementation
evidence as the remaining distance to §1.

## Acceptance

1. From a Windows desktop, `polariton act` connects a running window to a
   Linux Toron through `connect`, and the receipt carries the completed
   outcome and the database connection generation; the same command with a
   partial TLS configuration, a mismatched server name, and an untrusted
   client certificate returns the refusal the form would show, distinctly
   for each.
2. `connection_status` reports the generation the receipt named; a
   `disconnect` naming an older generation is refused; a connect result
   delayed past a newer human choice does not install itself.
3. A test drives the form and the command through the same connection
   operation and observes identical adapter state and identical receipts,
   on Linux and on Windows.
4. The Windows pipe refuses a client from another user account, with the
   refusal reported the same way the Unix socket reports it, and a client
   refuses a pipe whose server process is not the advertised one.

This ADR closes on those four. ADR 0165's acceptance 1 is then completed
through this surface once tenant provisioning lands, keeping its form-path
check as a separate, explicitly human step; that completion is ADR 0165's
evidence, not this ADR's.

## Limits and follow-up

This cut does not add commands for profile management, settings, session
checkpoints, or window layout; the evidence lists them. A remote (not
same-user) agent connection is a different security design and not
decided here. Keystroke or pointer automation of the window is not a
supported path and no tooling for it lives in the repository.
