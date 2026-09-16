# Chusan LAN Lab

Chusan LAN Lab checks the local-network paths used by `chusan` without starting the game. Run one instance per cabinet and use the browser GUI to configure the cabinet, start the node, recruit, and join.

It tests whether the PCs are ready for `chusan` cabinet-to-cabinet play, which depends on reliable local-network connectivity.

The tool intentionally uses a clearly marked JSON experiment protocol. It mirrors the known socket directions, roles, timing, and state transitions, but it does not claim binary compatibility with the game's AES/Boost packets.

## What it checks

- LAN Install discovery and sync: UDP `40112` and TCP `40110`.
- Setting Parent/Child discovery and state exchange: UDP/TCP `50201`.
- Advertise discovery: UDP `50202` (`Request`, `Response`, and `Go` when a peer replies).
- Party recruitment and joining: UDP/TCP `50200`.
- Limited broadcast, directed broadcast, or explicit unicast targets.
- Source-interface selection and the advertised Host IPv4 address.
- TCP connect, application exchange, member synchronization, and bidirectional heartbeats.
- Group, Event Mode, music ID, duplicate-join, and four-member capacity checks.

## Requirements

- Windows is the primary target because the original cabinet uses Windows and the tool configures Windows UDP broadcast sockets.
- Go 1.23 or newer.
- A modern browser on the same machine for the local GUI.

The program only listens for its GUI on `127.0.0.1`. The LAN services bind the configured test ports.

## Build and run

From the repository root:

```bat
scripts\build.bat
build\chusan-lan-lab.exe
```

For development:

```bat
scripts\run.bat
```

The default GUI URL is:

```text
http://127.0.0.1:18080/
```

Useful command-line options:

```text
-listen 127.0.0.1:18080   GUI HTTP address
-no-open                   do not open a browser automatically
```

## Recommended two-cabinet test

1. Run one instance on each machine and give the nodes different names.
2. Select the same group (`A`–`D`), Event Mode, music ID, and cabinet mode (`SP` or `CVT`).
3. Configure the cabinet roles independently:
   - exactly one `Parent`, the others `Child`;
   - exactly one LAN Install `Server`, the others `Client`.
4. Select the IPv4 address of the interface that should carry the test traffic. Start with `255.255.255.255` as the UDP target.
5. Click **Start node and run startup flow**. The tool runs:

   ```text
   LAN Install beacon -> LAN sync -> SettingHostAddress -> Setting TCP -> Advertise
   ```

6. After both nodes become ready, click **Start local recruitment** on one node.
7. The Host performs a loopback self-join. The other node should receive an invitation; click **Join**, or enable automatic join before starting.
8. A successful join produces the following experiment trace:

   ```text
   Hello
   ClientState x2
   RequestJoin
   JoinResult=Success
   PartyMemberInfo x2
   PartyMemberState x2
   UpdateUserInfo
   Bidirectional HeartBeatRequest/Response x10
   ```

When the group is `OFF`, the tool does not assign Parent/Child and does not expose Setting or Party recruitment. LAN Install still requires an explicit `Server` or `Client` identity.

## Port map

| Service | UDP | TCP | Purpose |
|---|---:|---:|---|
| LAN Install | `40112` | `40110` | amdaemon-style discovery and sync path |
| Party | `50200` | `50200` | recruitment, join, members, and heartbeats |
| Setting | `50201` | `50201` | Parent address discovery and group state |
| Advertise | `50202` | — | idle-advertising coordination |

Allow these ports in both directions on the test network. If Windows Firewall has per-application rules, also allow the actual game executables separately; allowing this tool does not automatically allow `chusanApp.exe` or `amdaemon.exe`.

## Reading the result

- Stuck at **Waiting for LAN Install beacon**: UDP `40112` discovery or TCP `40110` sync is unavailable.
- Beacon received but no sync response: discovery works; inspect TCP `40110`, firewall, and the return route.
- Stuck at **Waiting for Parent's SettingHostAddress**: no same-group Parent was received.
- Setting address received but no TCP response: inspect the advertised Host IPv4 and TCP `50201`.
- No invitation: inspect UDP `50200`, the broadcast egress, group, and VLAN/client isolation.
- Invitation visible but join does not progress: inspect Host address reachability and TCP `50200`.
- `DifferentGroup`, `DifferentEventMode`, or `DifferentMusic`: the network path is working and the simulated state values disagree.
- `CONNECT_PENDING` followed by `CONNECT_TIMEOUT`: the TCP state machine did not complete within 60 simulated update ticks.
- `MULTIPLE_SERVERS`: a LAN Install Client saw more than one distinct Server address.

The advanced diagnostics panel retains manual UDP/TCP probes for layered troubleshooting. They are not required for the normal cabinet-style workflow.

## Game-fidelity boundary

The workflow is based on the current reverse-engineering evidence:

- StartRecruit sends two initial frames about 51 ms apart and then repeats about every 6 seconds.
- SettingHostAddress repeats about every 3 seconds in the observed Parent log.
- The Host performs a loopback Party self-join.
- Party join checks group, Event Mode, music, recruitment state, capacity, and duplicate membership.
- The game's Party connect path enables non-blocking mode and treats pending connection states separately from immediate success and failure. The tool records `CONNECT_PENDING`, `CONNECT_READY`, and `CONNECT_TIMEOUT` with a 60-tick limit; SP uses 120 Hz timing and CVT uses 60 Hz timing.
- Setting maintains a TCP session and exchanges periodic heartbeats. Advertise continues with Response/Go when a peer is available.

The following remain outside the experiment protocol:

- The game's AES-128/Boost binary packet framing and exact payload fields.
- Complete `amdaemon` beacon contents, duplicate-server arbitration, resource distribution, and IPC readiness.
- Party `RequestMeasure`, `StartPlay`, in-game synchronization, result, and finish phases.
- Exact socket options, route hooks, `netenv` behavior, launcher behavior, and per-process firewall policy.

A failed lab run indicates a missing network prerequisite. A fully successful run is strong evidence that the relevant network paths work, but it does not certify binary compatibility or guarantee that the game will run perfectly.

## Development checks

```bat
gofmt -w .
go test ./...
go vet ./...
go build -trimpath -ldflags="-s -w" -o build\chusan-lan-lab.exe .
```

The project uses only the Go standard library. The GUI is embedded into the executable from `web/`.

## License
[MIT](LICENSE)
