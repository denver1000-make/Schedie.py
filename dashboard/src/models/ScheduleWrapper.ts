export interface ScheduleWrapper {
    schedule_id: string;
    upload_date_epoch: bigint;
    is_temporary: boolean;
    is_synced_to_remote: boolean;
    is_from_remote: boolean;
    created_at: Date;
    updated_at: Date;
}

export class ScheduleWrapperModel {
    static tableName = 'schedule_wrappers';

    static createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.schedule_wrapper
        (
            id serial NOT NULL,
            schedule_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
            created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
            upload_date_epoch bigint,
            total_slots integer DEFAULT 0,
            is_active boolean DEFAULT true,
            metadata jsonb DEFAULT '{}',
            CONSTRAINT schedule_wrapper_pkey PRIMARY KEY (id),
            CONSTRAINT schedule_wrapper_schedule_id_key UNIQUE (schedule_id)
        );

        CREATE INDEX IF NOT EXISTS idx_schedule_wrapper_created_at
            ON public.schedule_wrapper USING btree
            (created_at DESC NULLS LAST);

        CREATE INDEX IF NOT EXISTS idx_schedule_wrapper_active
            ON public.schedule_wrapper USING btree
            (is_active ASC NULLS LAST);
    `;

    static async findAll(client: any): Promise<ScheduleWrapper[]> {
        const query = `SELECT * FROM ${this.tableName} ORDER BY upload_date_epoch DESC`;
        const result = await client.query(query);
        return result.rows;
    }

    static async findMostRecent(client: any): Promise<ScheduleWrapper | null> {
        const query = `
            SELECT * FROM ${this.tableName} 
            ORDER BY upload_date_epoch DESC 
            LIMIT 1
        `;
        const result = await client.query(query, []);
        return result.rows[0] || null;
    }

    static async findMostRecentPermanent(client: any): Promise<ScheduleWrapper | null> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE is_temporary = false 
            ORDER BY upload_date_epoch DESC 
            LIMIT 1
        `;
        const result = await client.query(query);
        return result.rows[0] || null;
    }

    static async findActiveTemporary(client: any): Promise<ScheduleWrapper[]> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE is_temporary = true 
            ORDER BY upload_date_epoch DESC
        `;
        const result = await client.query(query);
        return result.rows;
    }

    static async findByScheduleId(client: any, scheduleId: string): Promise<ScheduleWrapper | null> {
        const query = `SELECT * FROM ${this.tableName} WHERE schedule_id = $1`;
        const result = await client.query(query, [scheduleId]);
        return result.rows[0] || null;
    }

    static async create(client: any, data: Omit<ScheduleWrapper, 'created_at' | 'updated_at'>): Promise<ScheduleWrapper> {
        const query = `
            INSERT INTO ${this.tableName} (schedule_id, upload_date_epoch, is_temporary, is_synced_to_remote, is_from_remote)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        `;
        const values = [
            data.schedule_id,
            data.upload_date_epoch,
            data.is_temporary || false,
            data.is_synced_to_remote !== undefined ? data.is_synced_to_remote : true,
            data.is_from_remote !== undefined ? data.is_from_remote : true
        ];
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async update(client: any, scheduleId: string, data: Partial<ScheduleWrapper>): Promise<ScheduleWrapper | null> {
        const fields = Object.keys(data).filter(key => key !== 'schedule_id' && key !== 'created_at' && key !== 'updated_at');
        const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
        const values = [scheduleId, ...fields.map(field => (data as any)[field])];

        const query = `
            UPDATE ${this.tableName}
            SET ${setClause}, updated_at = CURRENT_TIMESTAMP
            WHERE schedule_id = $1
            RETURNING *
        `;
        const result = await client.query(query, values);
        return result.rows[0] || null;
    }

    static async delete(client: any, scheduleId: string): Promise<boolean> {
        const query = `DELETE FROM ${this.tableName} WHERE schedule_id = $1`;
        const result = await client.query(query, [scheduleId]);
        return result.rowCount > 0;
    }
}