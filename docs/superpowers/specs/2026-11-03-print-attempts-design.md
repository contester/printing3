# Print attempts: separate results per try, reuse the rendered file

Spans three repos: `qf-webapp` (and `contester-dbmodel`, which holds its
table definitions), `printing3/busyprint`, `printing3/printagent`.

## Why

A print submission is one row in `print_jobs`. Printing it again re-queues
the same `s-<id>`; the results (`printed`, `pages`, `error`) are written
into the same row. Consequences:

- The history is lost. After "fail, fix, reprint, fail again" there is one
  error, and nobody can tell whether it's the old or the new one.
- Reports are matched by `s-<id>` only. A late report of attempt 1 (say a
  timeout) arriving after attempt 2 succeeded overwrites attempt 2's result.
- Every reprint renders again (pygments, LaTeX), so the paper can differ from
  the first try if the template, team name or tools changed, and a failure
  that was only about the printer still pays for the render.
- A timeout leaves the job in the Windows queue, so a reprint can produce two
  physical copies, and nothing records that this was possible.

## Goals

- Every try is an *attempt* with its own id, state and result; the history is
  kept and shown.
- A report can only affect the attempt it belongs to.
- If an attempt rendered but failed to print, the next attempt reuses the
  rendered file: same paper, no render.
- Rendering again stays possible, as an explicit choice.

## Non-goals

- Cancelling a stuck job in the Windows spooler (the agent does not delete
  jobs it timed out on). The UI should only say that a timed-out job may
  still print.
- Automatic retries, or timers that mark silent attempts as failed.
- Moving the rendered files to advfiler. Files are tens of KB; they live in
  the database. (A content reference can replace the bytes later.)
- Changing how teams submit, or what the team page shows by default.

## Anti-goals

- Breaking existing readers: `print_jobs.printed/pages/error` and the queries
  on them (`printJobsForTeam`, the admin list) keep working and keep meaning
  "the state of the latest attempt".
- A deploy order requirement between the three programs, or a flag day.
- Two attempts of one job being able to change each other's results.

## Model

### Tables (defined in `contester-dbmodel`, used by `qf-webapp`)

`print_renders` — one row per render of a job.

| column | |
|---|---|
| id | bigserial |
| job_id | -> print_jobs.id |
| format | `ps` or `pdf` |
| pages | page count at render time |
| data | the bytes |
| created | timestamp |

`print_attempts` — one row per try.

| column | |
|---|---|
| id | bigserial; this is the correlation id in the pipeline |
| job_id | -> print_jobs.id |
| kind | `full` (render, then print) or `print_only` (reuse `render_id`) |
| created | timestamp |
| requested_by | who clicked, null for the first attempt of a submission |
| printer | printer name used (copied from the area at creation) |
| render_id | -> print_renders.id; null until the render is reported |
| status | `queued`, `rendered`, `printed`, `failed` |
| finished | timestamp of the final report, null before |
| pages_printed | from the agent report, 0 if unknown |
| spooler_job_id | from the agent report, null if unknown |
| error | text, empty unless failed |
| detail | text from the agent |

`print_jobs` is unchanged. Its `printed`, `pages`, `error` are updated from a
final report only when that report's attempt is the job's latest attempt.
"Silent for a long time" is shown by the UI from `created`; it is not a state.

### Status transitions

```
full:        queued -> rendered -> printed | failed
             queued -> failed                       (render failed)
print_only:  rendered -> printed | failed           (created with render_id)
```

### The correlation id

`job_id` stays an opaque string in `PrintJob`, `TexJob`, `BinaryJob` and
`PrintJobReport`; its value is now `a-<attempt id>`. busyprint and printagent
already treat it as opaque (printagent puts it in a file name, so it must stay
`[A-Za-z0-9-]`). The webapp resolves `a-N` to the attempt. A report with the
old form `s-<job id>` (in flight during the deploy) is applied to the latest
attempt of that job.

## Protocol (`tickets.proto`, both copies)

`PrintJobReport` gets:

- `stage = 8` (enum): `FINAL = 0` (the default, so old programs' reports mean
  what they meant before: success if `error_message` is empty, failure
  otherwise) and `RENDERED = 1`. A stage a reader doesn't know is ignored.
- `BinaryJob render = 9`: set only on `RENDERED` reports; the payload, page
  count and format of the render. `printer` and `job_id` in it are the
  attempt's.

Nothing else changes; the fields added earlier (`detail`, `pages_printed`,
`spooler_job_id`) feed the attempt row.

## Flows

**Full attempt (first submission, or "re-render").**
1. Webapp inserts the attempt (`full`, `queued`) and queues the `PrintJob`
   to `contester.protoprintjobs` with `job_id = a-<id>`.
2. busyprint renders. On success it sends a `RENDERED` report to
   `contester.printingresult` carrying the `BinaryJob`, and then that
   `BinaryJob` to the agent's queue, both in one STOMP transaction with the
   ack of the incoming job. The order matters: the agent's `FINAL` report
   goes to the same result queue, so it can't overtake `RENDERED`. On a render failure busyprint sends a `FINAL` report with
   `error_message`, as today.
   The attempt id was allocated in step 1 and is carried unchanged through
   `PrintJob`, `BinaryJob` and both reports. The render has no id of its own
   in any message.
3. Webapp on `RENDERED`: inserts a `print_renders` row, sets the attempt's
   `render_id` and `status = rendered`.
4. Agent prints and sends its `FINAL` report. A `FINAL` that finds no render
   on its attempt (a broken ordering) is still applied; only the render link
   is missing. Webapp sets `printed` or
   `failed` and the agent's fields on that attempt, and updates `print_jobs`
   if this is the latest attempt.

**Print-only attempt ("reprint").**
1. Webapp inserts the attempt (`print_only`, `rendered`, `render_id` = the
   job's newest render) and publishes a `BinaryJob` built from that render
   straight to `contester.binaryproto`, with the printer taken from the
   area *now* and `job_id = a-<id>`.
2. Step 4 above. busyprint is not involved.

**Which one a reprint button creates.** `print_only` if the job has a render,
otherwise `full`. The admin gets a separate "re-render and print" that always
creates a `full` attempt. A new render never replaces the old one; older
attempts keep pointing at the render they printed.

## Components

- `contester-dbmodel`: the two tables and their Slick definitions.
- `qf-webapp`: `PrintingModel` creates attempts, builds the print-only
  `BinaryJob`, handles `RENDERED` and `FINAL` reports per attempt;
  `reprintJob` becomes the two actions above; the admin job list shows the
  attempts of a job, with a note on timeout failures that the job may still
  print. The team page keeps reading `print_jobs`.
- `busyprint`: send the `RENDERED` report after a successful render.
- `printagent`: no functional change; it already echoes `job_id` and fills
  the report.
- `tickets.proto` in `printing3` and in `qf-webapp`.

## Compatibility and rollout

All proto changes are additive. Any order works:

- New webapp, old busyprint: no `RENDERED` reports arrive, attempts have no
  render, reprints fall back to `full`. Nothing breaks, the feature is just
  inactive until busyprint is updated.
- New busyprint, old webapp: the extra report type has `stage` unknown to the
  old reader, which treats it as `FINAL` with an empty error, i.e. "printed".
  **That would be wrong**, so deploy the webapp first.
- Old agents work unchanged.

## Left to the builder

- How schema changes are applied to the database: no migration files were
  found in `contester-dbmodel` or `qf-webapp`. Follow the existing practice.
- Exact UI of the attempt list, and where `requested_by` comes from (the
  logged-in admin identity; null for team submissions and the test-print
  form).
- Existing rows: `print_jobs` rows without attempts are fine to leave alone;
  the attempt list is empty for them. No backfill needed.

## Calls made without asking (check these)

- Rendered files go in a Postgres table, not advfiler (the user agreed that
  small files make this fine).
- `print_jobs` keeps its summary columns instead of being derived from
  attempts, so existing queries stay as they are.
- The correlation id is `a-<attempt id>`; no new proto field for the attempt.
- `RENDERED` is a report stage with the `BinaryJob` inside it, rather than a
  new queue.
- No timer-driven "lost" state, and the agent does not cancel timed-out
  spooler jobs.
- Deploy webapp before busyprint (see above).

## Rollout

1. Apply `docs/superpowers/plans/print-attempts.sql` (needs `contester-dbmodel`
   with the new tables published for the webapp build).
2. Deploy `qf-webapp`.
3. Deploy busyprint (`tito build --rpm`). Until then no `RENDERED` reports
   arrive and reprints fall back to full attempts.
4. printagent needs no change.
