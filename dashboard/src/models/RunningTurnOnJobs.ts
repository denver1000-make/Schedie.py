export interface RunningTurnOnJobs {
    timeslot_id: string;
    is_temporary: boolean;
}

export class RunningTurnOnJobsModel {
    static tableName = 'running_turn_on_jobs';

    static createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.running_turn_on_jobs
        (
            id integer NOT NULL DEFAULT nextval('running_turn_on_jobs_id_seq'::regclass),
            timeslot_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
            is_temporary boolean DEFAULT false,
            created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT running_turn_on_jobs_pkey PRIMARY KEY (id),
            CONSTRAINT running_turn_on_jobs_timeslot_id_key UNIQUE (timeslot_id)
        );

        CREATE INDEX IF NOT EXISTS idx_running_jobs_is_temporary
            ON public.running_turn_on_jobs USING btree
            (is_temporary ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS idx_running_jobs_timeslot_id
            ON public.running_turn_on_jobs USING btree
            (timeslot_id COLLATE pg_catalog."default" ASC NULLS LAST);
    `;

    static async findAll(client: any): Promise<RunningTurnOnJobs[]> {
        const query = `SELECT * FROM ${this.tableName} ORDER BY timeslot_id`;
        const result = await client.query(query);
        return result.rows;
    }

    static async findByTimeslotId(client: any, timeslotId: string): Promise<RunningTurnOnJobs | null> {
        const query = `SELECT * FROM ${this.tableName} WHERE timeslot_id = $1`;
        const result = await client.query(query, [timeslotId]);
        return result.rows[0] || null;
    }

    static async findRunningJobs(client: any): Promise<Map<string, RunningTurnOnJobs>> {
        const query = `SELECT * FROM ${this.tableName} ORDER BY timeslot_id`;
        const result = await client.query(query);
        
        const runningJobsMap = new Map<string, RunningTurnOnJobs>();
        result.rows.forEach((job: RunningTurnOnJobs) => {
            runningJobsMap.set(job.timeslot_id, job);
        });
        
        return runningJobsMap;
    }

    static async create(client: any, data: RunningTurnOnJobs): Promise<RunningTurnOnJobs> {
        const query = `
            INSERT INTO ${this.tableName} (timeslot_id, is_temporary)
            VALUES ($1, $2)
            RETURNING *
        `;
        const values = [data.timeslot_id, data.is_temporary];
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async delete(client: any, timeslotId: string): Promise<boolean> {
        const query = `DELETE FROM ${this.tableName} WHERE timeslot_id = $1`;
        const result = await client.query(query, [timeslotId]);
        return result.rowCount > 0;
    }
}