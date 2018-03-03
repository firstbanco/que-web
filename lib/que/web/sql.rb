module Que
  class Web
    class << self
      def lock_job_query(condition)
        <<-SQL.freeze
          SELECT job_id, pg_try_advisory_lock(job_id) AS locked
          FROM que_jobs
          WHERE #{condition}
        SQL
      end

      def reschedule_all_jobs_query(scope)
        <<-SQL.freeze
          WITH target AS (#{scope})
          UPDATE que_jobs
          SET run_at = $1::timestamptz
          FROM target
          WHERE target.locked
          AND target.job_id = que_jobs.job_id
          RETURNING pg_advisory_unlock(target.job_id)
        SQL
      end

      def delete_jobs_query(scope)
        <<-SQL.freeze
          WITH target AS (#{scope})
          DELETE FROM que_jobs
          USING target
          WHERE target.locked
          AND target.job_id = que_jobs.job_id
          RETURNING pg_advisory_unlock(target.job_id)
        SQL
      end

      def select_jobs_query(condition)
        <<-SQL.freeze
          SELECT que_jobs.*
          FROM que_jobs
          LEFT JOIN (
            #{ADVISORY_LOCKS_SQL}
          ) locks USING (job_id)
          WHERE locks.job_id IS NULL AND #{condition} AND job_class LIKE ($3)
          ORDER BY run_at
          LIMIT $1::int
          OFFSET $2::int
        SQL
      end
    end

    LOCK_JOB_SQL = lock_job_query('job_id = $1::bigint').freeze

    LOCK_ALL_FAILING_JOBS_SQL = lock_job_query('error_count > 0').freeze

    LOCK_ALL_SCHEDULED_JOBS_SQL = lock_job_query('error_count = 0').freeze

    ADVISORY_LOCKS_SQL = <<-SQL.freeze
      SELECT (classid::bigint << 32) + objid::bigint AS job_id
      FROM pg_locks
      WHERE locktype = 'advisory'
    SQL

    SQL = {
      dashboard_stats: <<-SQL.freeze,
        SELECT count(*)                    AS total,
               count(locks.job_id)         AS running,
               coalesce(sum((error_count > 0 AND locks.job_id IS NULL)::int), 0) AS failing,
               coalesce(sum((error_count = 0 AND locks.job_id IS NULL)::int), 0) AS scheduled
        FROM que_jobs
        LEFT JOIN (
          #{ADVISORY_LOCKS_SQL}
        ) locks USING (job_id)
        WHERE
          job_class LIKE ($1)
      SQL
      failing_jobs: select_jobs_query('error_count > 0'),
      scheduled_jobs: select_jobs_query('error_count = 0'),
      delete_job: delete_jobs_query(LOCK_JOB_SQL),
      delete_all_scheduled_jobs: delete_jobs_query(LOCK_ALL_SCHEDULED_JOBS_SQL),
      delete_all_failing_jobs: delete_jobs_query(LOCK_ALL_FAILING_JOBS_SQL),
      reschedule_job: <<-SQL.freeze,
        WITH target AS (#{LOCK_JOB_SQL})
        UPDATE que_jobs
        SET run_at = $2::timestamptz
        FROM target
        WHERE target.locked
        AND target.job_id = que_jobs.job_id
        RETURNING pg_advisory_unlock(target.job_id)
      SQL
      reschedule_all_scheduled_jobs: reschedule_all_jobs_query(LOCK_ALL_SCHEDULED_JOBS_SQL),
      reschedule_all_failing_jobs: reschedule_all_jobs_query(LOCK_ALL_FAILING_JOBS_SQL),
      fetch_job: <<-SQL.freeze,
        SELECT *
        FROM que_jobs
        WHERE job_id = $1::bigint
        LIMIT 1
      SQL
    }.freeze
  end
end
