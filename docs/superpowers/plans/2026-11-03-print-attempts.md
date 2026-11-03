# Print Attempts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every print try is an attempt with its own result; a reprint of a rendered job reuses the stored render.

**Architecture:** The webapp allocates an attempt row, sends its id (`a-<id>`) through the unchanged pipeline as the opaque job id, and matches reports to attempts. busyprint additionally reports `RENDERED` (carrying the `BinaryJob`) so the webapp can store the render and later publish print-only attempts straight to the agent.

**Tech Stack:** Go (printing3: protobuf, STOMP), Scala 2.12 / Play / Slick / ScalaPB / op-rabbit (qf-webapp, contester-dbmodel), PostgreSQL, scalatest.

**Spec:** `docs/superpowers/specs/2026-11-03-print-attempts-design.md` (in `/home/stingray/m/printing3`)

Repos: `P` = `/home/stingray/m/printing3`, `W` = `/home/stingray/m/qf-webapp`, `D` = `/home/stingray/m/contester-dbmodel`.

## Global Constraints

- Correlation id is `a-<attempt id>`; allowed characters `[A-Za-z0-9-]` (printagent puts it in a file name). Legacy `s-<job id>` reports apply to the job's latest attempt (highest attempt id).
- Statuses (exact strings): `queued`, `rendered`, `printed`, `failed`. Kinds: `full`, `print_only`. Formats: `ps`, `pdf`.
- `PrintJobReport.stage`: `FINAL = 0` (default), `RENDERED = 1`; `BinaryJob render = 8` on `PrintJobReport`.
- `print_jobs.printed/pages/error` are updated from a FINAL report only if its attempt is the job's latest.
- All proto changes additive; the two `tickets.proto` copies stay field-for-field identical.
- The user applies SQL by hand; no migration tooling, no schema auto-creation.
- Deploy webapp before busyprint.
- Commits end with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.

## Review Focus

- A late FINAL of attempt 1 after attempt 2 finished must not change attempt 2 or `print_jobs` (Task 4 test).
- A redelivered `RENDERED` must not insert a second render (Task 4 test).
- `FINAL` arriving before `RENDERED` is applied; the render still attaches afterwards without changing a terminal status (Task 4 test).
- A report for an unknown attempt or an unparsable id is acked and ignored, never thrown (Task 4/5).
- Reprint when the area has no printer, or the job has no render: error / falls back to `full` (Task 5).
- A `RENDERED` report with no `render` message is ignored (Task 4 test).

---

### Task 1: Tables and SQL

**Files:**
- Create: `P/docs/superpowers/plans/print-attempts.sql`
- Modify: `D/src/main/scala/Model.scala` (next to `dbPrintJobs`, ~line 457)

**Interfaces:**
- Produces (in `SlickModel`, package `org.stingray.contester.dbmodel`):
  - `case class DBPrintRender(id: Option[Long], jobId: Long, format: String, pages: Int, data: Array[Byte], created: DateTime)`; `dbPrintRenders: TableQuery[DBPrintRenders]`, table `print_renders`.
  - `case class DBPrintAttempt(id: Option[Long], jobId: Long, kind: String, created: DateTime, requestedBy: Option[String], printer: String, renderId: Option[Long], status: String, finished: Option[DateTime], pagesPrinted: Int, spoolerJobId: Option[Long], error: String, detail: String)`; `dbPrintAttempts: TableQuery[DBPrintAttempts]`, table `print_attempts`.

- [ ] **Step 1: Write `print-attempts.sql`** for PostgreSQL, to be applied by hand, in this order: `CREATE TABLE print_renders` (id bigserial PK, job_id bigint NOT NULL, format text NOT NULL, pages integer NOT NULL, data bytea NOT NULL, created timestamp NOT NULL DEFAULT now()); `CREATE TABLE print_attempts` (id bigserial PK, job_id bigint NOT NULL, kind text NOT NULL, created, requested_by text NULL, printer text NOT NULL, render_id bigint NULL, status text NOT NULL, finished NULL, pages_printed integer NOT NULL DEFAULT 0, spooler_job_id bigint NULL, error text NOT NULL DEFAULT '', detail text NOT NULL DEFAULT ''); then `ALTER TABLE ... ADD CONSTRAINT` for: both `job_id` FKs to `print_jobs(id)`, `print_attempts.render_id` FK to `print_renders(id)`, CHECKs `kind IN ('full','print_only')`, `status IN ('queued','rendered','printed','failed')`, `format IN ('ps','pdf')`; then `CREATE INDEX` on `print_attempts(job_id)` and `print_renders(job_id)`. A header comment tells the user to run `\d print_jobs` first and use the same type for timestamp columns as `print_jobs.arrived` (the file uses `timestamp`; change if `arrived` is `timestamptz`). Add a commented-out rollback block (`DROP TABLE print_attempts; DROP TABLE print_renders;`).
- [ ] **Step 2: Add the two case classes and `Table` definitions** to `Model.scala` following `DBPrintJobs` (column names exactly as in the SQL; `created`/`finished` use the same DateTime column mapping as `arrived`/`printed`).
- [ ] **Step 3: Compile and publish** — `cd D && sbt compile publishLocal`. Expected: success, snapshot `2024.0.1-SNAPSHOT` published locally (the webapp depends on it).
- [ ] **Step 4: Commit** `D` ("Add print_attempts and print_renders tables") and `P` (the SQL file).

### Task 2: Protocol

**Files:**
- Modify: `P/tickets/tickets.proto`, regenerate `P/tickets/tickets.pb.go` (`cd P/tickets && go generate ./...`)
- Modify: `W/app/protobuf/Tickets.proto`

**Interfaces:**
- Produces: in both protos, inside `PrintJobReport`: `enum Stage { FINAL = 0; RENDERED = 1; }`, `Stage stage = 8;`, `BinaryJob render = 9;`. (Field numbers 8 and 9; 5–7 are taken.) Go: `tpb.PrintJobReport_FINAL`, `tpb.PrintJobReport_RENDERED`, `Stage`, `Render` fields. Scala: `PrintJobReport.Stage.RENDERED`, `render: Option[BinaryJob]`.

- [ ] **Step 1: Edit both proto files identically** (comment: `render` is set only when `stage == RENDERED`; `printer` and `job_id` in it are the attempt's).
- [ ] **Step 2: Regenerate Go** and `go build ./... && go test ./...` in `P`. Expected: PASS (the spec text in the design doc said field 8 for `render`; this plan fixes 8 = stage, 9 = render — update the design doc's Protocol section to match).
- [ ] **Step 3: `cd W && sbt "set offline := true" compile`.** Expected: success.
- [ ] **Step 4: Commit** in `P` and `W` ("Report stage and render in PrintJobReport").

### Task 3: busyprint sends RENDERED

**Files:**
- Modify: `P/tools/receive.go`, `P/busyprint/main.go` (`processTexJob`)
- Create: `P/busyprint/report.go`, `P/busyprint/report_test.go`

**Interfaces:**
- Produces: `tools.Outgoing struct { Dest string; Msg proto.Message }`; `tools.SendAllAndAck(msg *stomp.Message, outs ...Outgoing) error` — one STOMP transaction: sends in order, acks, commits (same error handling as `SendAndAck`, which becomes a call to it with one `Outgoing`). `renderedReport(job *tpb.BinaryJob) *tpb.PrintJobReport` in `busyprint/report.go`.

- [ ] **Step 1: Test** `TestRenderedReport`: for `&tpb.BinaryJob{JobId:"a-7", Printer:"p", Pages:3, Payload:&tpb.BinaryJob_Pdf{Pdf:[]byte("x")}}` the report has `JobExpandedId=="a-7"`, `Stage==PrintJobReport_RENDERED`, `NumPages==3`, `TimestampSeconds>0`, `GetRender()` proto-equal to the input, and `ErrorMessage==""`.
- [ ] **Step 2: Run** `go test ./busyprint -run TestRenderedReport` → FAIL (undefined).
- [ ] **Step 3: Implement `renderedReport`.**
- [ ] **Step 4: Implement `tools.SendAllAndAck`** and make `SendAndAck` delegate; in `processTexJob` replace the final `SendAndAck(msg, s.BinaryQueue, &bpb)` with `SendAllAndAck(msg, Outgoing{s.FailureQueue, renderedReport(&bpb)}, Outgoing{s.BinaryQueue, &bpb})` — report first (design: ordering), one transaction.
- [ ] **Step 5: Run** `go vet ./... && go test ./...` → PASS.
- [ ] **Step 6: Commit** ("busyprint: report RENDERED before handing the job to the agent"). Also change the design doc's flow step 2 to say the two sends are one transaction.

### Task 4: Report handling logic (pure, tested)

**Files:**
- Create: `W/app/models/PrintAttempts.scala`, `W/test/models/PrintAttemptsSpec.scala`

**Interfaces:**
- Produces, `object PrintAttempts` in package `models`:
  - `sealed trait JobRef`; `case class AttemptRef(id: Long) extends JobRef`; `case class LegacyJobRef(jobId: Long) extends JobRef`; `def parseJobId(s: String): Option[JobRef]` (`a-<digits>` → AttemptRef, `s-<digits>` → LegacyJobRef, else None).
  - `object Status { val Queued, Rendered, Printed, Failed: String }` (strings per Global Constraints); `def isTerminal(status: String): Boolean`.
  - `case class AttemptState(status: String, hasRender: Boolean)`.
  - `sealed trait Action`; `case object Ignore`; `case class AttachRender(render: BinaryJob, advanceStatus: Boolean)`; `case class Finish(printed: Boolean)`.
  - `def decide(state: AttemptState, report: PrintJobReport): Action`.

- [ ] **Step 1: Write `PrintAttemptsSpec`** (scalatest `AnyFunSuite`, style of `ComposeTeamNameSpec`) with tests: `parseJobId` accepts `a-12`, `s-3`, rejects `a-`, `x-1`, `a-1x`, `""`; `decide`: RENDERED on `Queued/no render` → `AttachRender(_, true)`; RENDERED when `hasRender` → `Ignore` (redelivery); RENDERED without `render` message → `Ignore`; RENDERED on terminal status without render → `AttachRender(_, false)`; FINAL with empty error on `Rendered` → `Finish(true)`; FINAL with error on `Queued` → `Finish(false)`; FINAL on terminal status → `Ignore`.
- [ ] **Step 2: Run** `cd W && sbt "set offline := true" "testOnly models.PrintAttemptsSpec"` → FAIL (does not compile).
- [ ] **Step 3: Implement** the object per the Interfaces block.
- [ ] **Step 4: Run** the same command → PASS.
- [ ] **Step 5: Commit** in `W` ("Pure logic for matching print reports to attempts").

### Task 5: PrintingModel uses attempts

**Files:**
- Modify: `W/app/models/SlickModel.scala` (`PrintingModel`), `W/app/models/RabbitMqModel.scala` only if the report handler signature changes (it should not).

**Interfaces:**
- Consumes: Task 1 tables, Task 2 protos, Task 4 `PrintAttempts`.
- Produces on `PrintingModel`:
  - `createFullAttempt(jobId: Long, requestedBy: Option[String]): Future[Option[Long]]` — inserts a `full`/`queued` attempt (printer copied from the job's area, as `buildProtoFromID` finds it), queues the `PrintJob` with `jobId = s"a-$attemptId"`; returns the attempt id, or None (and `announcePrintError`) when the job can't be built, as `printJobByID` does today.
  - `createPrintOnlyAttempt(jobId: Long, requestedBy: Option[String]): Future[Either[String, Long]]` — if the job has no render, delegates to `createFullAttempt`; otherwise inserts a `print_only`/`rendered` attempt with `renderId` = newest render of the job, publishes a `BinaryJob(printer = area's printer now, jobId = s"a-$id", pages = render pages, payload = ps|pdf per render format)` to `contester.binaryproto`; `Left(message)` if the area has no printer.
  - `processPrintJobReport(r: PrintJobReport): Future[Int]` — resolves the id with `parseJobId`; `LegacyJobRef` → latest attempt of the job (and legacy rows without attempts keep the old direct `print_jobs` update); unknown/unparsable → returns 0 (acked, logged); otherwise loads the attempt, `decide`s, and in one transaction applies the action: `AttachRender` inserts a `print_renders` row, sets `render_id`, and sets status `rendered` when `advanceStatus`; `Finish` sets status, `finished`, `pages_printed`, `spooler_job_id`, `error`, `detail`, and updates `print_jobs.printed/pages/error` (as today's code does, `announcePrintError` on error) only when this is the job's highest attempt id.
  - `insertAndPrintJob` and the old `printJobByID` callers use `createFullAttempt` after inserting the job (the test-print form creates the first attempt).

- [ ] **Step 1: Extract the `PrintJob` building** from `buildProtoFromID` so the job id string is a parameter (`buildProto(id: Long, correlationId: String)`); existing behavior unchanged. `sbt compile` → success.
- [ ] **Step 2: Implement `createFullAttempt`, `createPrintOnlyAttempt`, and the new `processPrintJobReport`** per the Interfaces block. Atomicity: the report handler's DB writes run in one `DBIO` transaction (`.transactionally`).
- [ ] **Step 3: Update `insertAndPrintJob`** (and nothing else that queues jobs) to create the first attempt.
- [ ] **Step 4: Verify** `sbt "set offline := true" compile test` → success, all specs PASS.
- [ ] **Step 5: Commit** ("Print attempts: one row per try, reports matched per attempt").

### Task 6: Admin actions and view

**Files:**
- Modify: `W/conf/routes`, `W/app/controllers/AdminApplication.scala` (`reprintJob`, `getPrintJobs`, `AdminPrintJob`), `W/app/views/admin/printjobs.scala.html`

**Interfaces:**
- Consumes: Task 5 `createPrintOnlyAttempt`, `createFullAttempt`.
- Produces: route `POST /admin/reprintPrintJob/:id` (kept; now print-only) and new `POST /admin/rerenderPrintJob/:id` → `AdminApplication.rerenderJob(id: Long)`; `AdminPrintJob` gains `attempts: Seq[AdminPrintAttempt]` with `case class AdminPrintAttempt(id: Long, kind: String, created: DateTime, requestedBy: Option[String], printer: String, status: String, pagesPrinted: Int, error: String, detail: String)`.

- [ ] **Step 1: Controller:** `reprintJob` → `createPrintOnlyAttempt(id, Some(request.identity.<login field>))` ("ok" / 500 with the Left message); `rerenderJob` → `createFullAttempt`.
- [ ] **Step 2: `getPrintJobs`** also loads attempts of the listed jobs (one query by `job_id` in the page's job ids, newest first) and fills `attempts`.
- [ ] **Step 3: View:** per job, an expandable list of attempts (id, kind, time, status, pages printed, error, detail); a "may still print" note for attempts whose error contains `did not finish in time`; a "re-render and print" button next to the existing reprint button. Follow the existing markup and JS of the page for the reprint button.
- [ ] **Step 4: Verify** `sbt "set offline := true" compile test` → success; start the app if a dev database is available and open `/admin/<contest>/printJobs` to see the list render (otherwise state in the report that the view was only compiled).
- [ ] **Step 5: Commit** ("Admin: attempt history, print-only reprint, re-render").

### Task 7: Rollout notes

**Files:**
- Modify: `P/docs/superpowers/specs/2026-11-03-print-attempts-design.md` (Protocol field numbers per Task 2; busyprint transaction per Task 3)

- [ ] **Step 1: Update the design doc** as noted, and add a short "Rollout" list: apply `print-attempts.sql`; deploy webapp; then busyprint (RPM from `tito build --rpm`); printagent unchanged.
- [ ] **Step 2: Full check:** in `P`: `go vet ./... && GOOS=windows go vet ./printagent && go test ./...`; in `W`: `sbt "set offline := true" test`. Expected: all PASS.
- [ ] **Step 3: Commit** in `P`.
