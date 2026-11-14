# Assignment 5 Leader Worker Consensus

The leader sends an arithmetic task to connected workers. Each user calculates an answer and types it into a worker terminal. The leader counts the answers and announces whether one result has a strict majority. The workers do not calculate the task automatically.

## Files

| File | Purpose |
| --- | --- |
| `src/main/java/Leader.java` | Accept connections, send tasks, collect votes, and announce decisions |
| `src/main/java/Worker.java` | Connect to the leader, accept manual input, and display decisions |
| `build.gradle` | Compile the code and provide runLeader and runWorker tasks |
| `settings.gradle` | Set the project name |
| `README.md` | Explain the setup and protocol |
| `gradlew`, `gradlew.bat`, `gradle/wrapper/` | Start the project with Gradle 8.13 |

## Compile and run

Use a full JDK, version 17 or 21. The included Gradle wrapper downloads Gradle 8.13 on its first run. Internet access is needed for that download. The project was checked with JDK 21 and uses only the Java standard library.

Extract the zip. Open four Command Prompt (CMD) windows in `Assignment5/activity1`. The Windows commands below use `gradlew.bat`. On Linux or macOS, use `./gradlew` in its place. First compile the code:

```cmd
.\gradlew.bat classes
```

In terminal 1, start the leader:

```cmd
.\gradlew.bat --console=plain runLeader --args="9000 30"
```

The arguments are the port and timeout in seconds. Both are optional; the defaults are port 9000 and 30 seconds.

In terminals 2, 3, and 4, start one worker each:

```cmd
.\gradlew.bat --console=plain runWorker --args="Worker1 localhost 9000"
```

```cmd
.\gradlew.bat --console=plain runWorker --args="Worker2 localhost 9000"
```

```cmd
.\gradlew.bat --console=plain runWorker --args="Worker3 localhost 9000"
```

After all three workers connect, enter `workers` at the leader to check the list. Then enter a task such as `15 + 27`. If fewer than three workers are connected, the task is rejected and must be entered again after they connect.

Workers use `roundId:answer` for manual input. For round 1, enter `1:42` to submit 42. Each task displays its round ID. Enter a new task at the leader for another round. Enter `quit` at the leader to stop the system, or at a worker to disconnect that worker. The leader reads commands between rounds, so a quit command entered during a response wait takes effect after the round ends.

### Two devices or AWS

Run the leader on the reachable device. Replace `localhost` in each remote worker command with the leader's address:

```cmd
.\gradlew.bat --console=plain runWorker --args="Worker1 LEADER_IP 9000"
```

Replace `LEADER_IP` with the actual address. Allow inbound TCP port 9000 from the worker device through the leader's firewall and AWS security group. Keep worker IDs unique. Local execution was checked; the AWS connection needs to be verified on the actual setup.

## Protocol

Workers open persistent TCP connections to the designated leader. Messages use UTF-8 text, tabs between fields, and a newline at the end. The fields below are listed in send order. Console logs display tabs as bars for readability.

| Message | Fields after the name | Purpose |
| --- | --- | --- |
| `HELLO` | `workerId` | Register a worker ID |
| `WELCOME` | `workerId` | Confirm registration |
| `TASK` | `roundId, timeoutSeconds, expression` | Assign a task |
| `RESULT` | `roundId, workerId, answer` | Submit the typed answer |
| `ACK` | `roundId, workerId, normalizedAnswer` | Confirm that a vote counted |
| `DECISION` | `roundId, status, result, largestCount, participantCount, responseCount, distribution` | Announce the outcome |
| `ERROR` | `roundId, explanation` | Reject a message; round 0 means no specific round |
| `BYE` | `explanation` | Announce leader shutdown |

The decision status is `CONSENSUS` or `NO_CONSENSUS`. The result field is `-` when there is no consensus. `largestCount` is the highest number of matching votes. A distribution such as `{42=2, 43=1}` means two workers voted 42 and one voted 43. An empty distribution is `{}`.

Each worker ID is tied to its connection. Duplicate IDs are rejected. The last vote can cause ACK and DECISION to arrive in either order; both messages identify the round.

## Consensus and threads

At the start of each round, `runRound` copies all connected workers into a fixed participant set. There must be at least three. With N participants, the required votes are `N / 2 + 1`, using integer division.

Each connection has a reader thread from the `readers` thread pool, so workers can respond concurrently. `stateLock` protects the worker list and round data. The leader's timed wait releases that lock while waiting, letting reader threads record votes and signal progress with `notifyAll()`.

`receiveResult` checks the connection, worker ID, round ID, participant membership, deadline, duplicate status, and numeric format. Only the first valid response from a participant counts. An invalid response does not use up that worker's vote. Numeric forms such as `42` and `42.0` are treated as the same value.

The leader stops waiting when all participants have answered or the deadline expires. It continues waiting for the remaining responses even if a majority is already visible. The deadline starts just before task dispatch and uses `System.nanoTime()`. Only responses accepted before that deadline and before the round closes can count.

After closing the round, the leader counts matching answers, prints the distribution, and sends the outcome to all currently connected workers. Without a strict majority, it announces `No consensus reached`. New connections can see the announcement but cannot vote until the next round.

| Participants | Responses | Outcome |
| --- | --- | --- |
| 3 | `42, 42, 43` | Consensus 42 with 2/3 |
| 3 | `42, 41, 43` | No consensus |
| 3 | `42, 42, no response` | Consensus 42 with 2/3 after timeout |
| 3 | `42, no response, no response` | No consensus after timeout |
| 4 | `42, 42, no response, disconnected` | No consensus; 2/4 is only half |

## Worker failures

A silent or disconnected worker still counts in the current round's participant total. A vote accepted before disconnection stays counted. A disconnected worker is removed from the connection list for future rounds. Reconnecting creates a different connection and does not replace the old connection in a round already running.

The worker reads keyboard input on a separate thread, so it can display a decision or disconnection while its user has entered nothing. Connection setup and registration have five-second limits. Normal leader shutdown sends BYE and closes the connections. Unexpected socket closure is also handled without leaving the worker blocked on keyboard input.

## Input handling issue

The input issue was how to receive a decision while a worker was waiting for a typed answer. Following the keyboard and network reads showed that using the same thread would delay the decision until the user pressed Enter.

A separate console thread lets the network receiver keep running. Requiring `roundId:answer` also identifies which task an answer belongs to if the user finishes after a timeout. Both programs check the round ID, so an old answer cannot become a vote in a later round.

## Edge cases and limitations

Late responses, duplicate votes, wrong round IDs, invalid numbers, and mismatched identities are rejected. Workers joining during a round participate in the next round. Agreement does not prove the answer is mathematically correct.

There is no leader election, saved state, automatic reconnection, heartbeat, authentication, or encryption. One person could open several different worker IDs. The response deadline covers missing answers, but blocking socket writes could still stall if a peer stops reading and the TCP buffers fill. The program is intended for a small set of cooperating workers.

Tasks accept two signed integers or decimals with one operator: `+`, `-`, or `*`. A leading `Calculate ` is optional. Division, parentheses, and longer expressions are rejected. Answers are limited to 100 characters without scientific notation. Worker IDs allow 1-32 letters, digits, underscores, or hyphens. Protocol lines are limited to 4096 characters.

## Video Demonstration

Video Link:(https://drive.google.com/file/d/14V6mqiYoawWjjUwej47F6ixF0AWsmONf/view?usp=sharing)
