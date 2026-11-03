-- Print attempts: apply by hand with psql, in this order.
--
-- Before running, check the column types of the existing table:
--     \d print_jobs
-- and use the same type for the timestamp columns below as print_jobs.arrived
-- uses. This file says "timestamp"; change it to "timestamptz" if arrived is.
-- print_jobs itself is not altered.

BEGIN;

CREATE TABLE print_renders (
    id       bigserial PRIMARY KEY,
    job_id   bigint    NOT NULL,
    format   text      NOT NULL,
    pages    integer   NOT NULL,
    data     bytea     NOT NULL,
    created  timestamp NOT NULL DEFAULT now()
);

CREATE TABLE print_attempts (
    id              bigserial PRIMARY KEY,
    job_id          bigint    NOT NULL,
    kind            text      NOT NULL,
    created         timestamp NOT NULL DEFAULT now(),
    requested_by    text,
    printer         text      NOT NULL,
    render_id       bigint,
    status          text      NOT NULL,
    finished        timestamp,
    pages_printed   integer   NOT NULL DEFAULT 0,
    spooler_job_id  bigint,
    error           text      NOT NULL DEFAULT '',
    detail          text      NOT NULL DEFAULT ''
);

ALTER TABLE print_renders
    ADD CONSTRAINT print_renders_job_fk FOREIGN KEY (job_id) REFERENCES print_jobs (id),
    ADD CONSTRAINT print_renders_format_ck CHECK (format IN ('ps', 'pdf'));

ALTER TABLE print_attempts
    ADD CONSTRAINT print_attempts_job_fk FOREIGN KEY (job_id) REFERENCES print_jobs (id),
    ADD CONSTRAINT print_attempts_render_fk FOREIGN KEY (render_id) REFERENCES print_renders (id),
    ADD CONSTRAINT print_attempts_kind_ck CHECK (kind IN ('full', 'print_only')),
    ADD CONSTRAINT print_attempts_status_ck CHECK (status IN ('queued', 'rendered', 'printed', 'failed'));

CREATE INDEX print_attempts_job_idx ON print_attempts (job_id);
CREATE INDEX print_renders_job_idx ON print_renders (job_id);

COMMIT;

-- Rollback:
--   DROP TABLE print_attempts;
--   DROP TABLE print_renders;
