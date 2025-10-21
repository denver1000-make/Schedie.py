export interface PowerUsage {
    timestamp: Date;
    room_id: string;
    power_watts: number;
    device_id?: string;
    voltage?: number;
    current?: number;
    frequency?: number;
    power_factor?: number;
    created_at: Date;
}

export class PowerUsageModel {
    static tableName = 'power_usage';

    static createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.power_usage (
            timestamp timestamp without time zone NOT NULL,
            room_id character varying(50) NOT NULL,
            power_watts integer NOT NULL,
            device_id character varying(100),
            voltage double precision,
            current double precision,
            frequency double precision,
            power_factor double precision,
            created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT power_usage_pkey PRIMARY KEY (timestamp, room_id)
        );

        -- Create TimescaleDB hypertable (if TimescaleDB is available)
        -- This will be executed conditionally in the database initialization
        -- SELECT create_hypertable('power_usage', 'timestamp', if_not_exists => TRUE);

        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_power_usage_room_id 
            ON public.power_usage USING btree (room_id);
        
        CREATE INDEX IF NOT EXISTS idx_power_usage_timestamp_room 
            ON public.power_usage USING btree (timestamp DESC, room_id);
        
        CREATE INDEX IF NOT EXISTS idx_power_usage_created_at 
            ON public.power_usage USING btree (created_at DESC);
    `;

    static async insertPowerUsage(client: any, data: Omit<PowerUsage, 'created_at'>): Promise<PowerUsage> {
        const query = `
            INSERT INTO ${this.tableName} (timestamp, room_id, power_watts, device_id, voltage, current, frequency, power_factor, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
            RETURNING *
        `;
        const values = [
            data.timestamp,
            data.room_id,
            data.power_watts,
            data.device_id || null,
            data.voltage || null,
            data.current || null,
            data.frequency || null,
            data.power_factor || null
        ];
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async getPowerUsageByRoomAndDateRange(
        client: any,
        roomId: string,
        startDate: Date,
        endDate: Date,
        limit?: number
    ): Promise<PowerUsage[]> {
        let query = `
            SELECT * FROM ${this.tableName}
            WHERE room_id = $1 AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp DESC
        `;
        
        const params = [roomId, startDate, endDate];
        
        if (limit) {
            query += ` LIMIT $4`;
            params.push(limit.toString());
        }
        
        const result = await client.query(query, params);
        return result.rows;
    }

    static async getPowerUsageSummaryByDateRange(
        client: any,
        startDate: Date,
        endDate: Date,
        roomIds?: string[]
    ): Promise<Array<{
        room_id: string;
        avg_watts: number;
        min_watts: number;
        max_watts: number;
        readings_count: number;
        total_watts: number;
    }>> {
        let query = `
            SELECT 
                room_id,
                AVG(power_watts)::numeric(10,2) as avg_watts,
                MIN(power_watts) as min_watts,
                MAX(power_watts) as max_watts,
                COUNT(power_watts) as readings_count,
                SUM(power_watts) as total_watts
            FROM ${this.tableName}
            WHERE timestamp >= $1 AND timestamp <= $2
        `;
        
        const params: any[] = [startDate, endDate];
        
        if (roomIds && roomIds.length > 0) {
            query += ` AND room_id = ANY($3::text[])`;
            params.push(roomIds);
        }
        
        query += ` GROUP BY room_id ORDER BY room_id`;
        
        const result = await client.query(query, params);
        return result.rows.map((row: any) => ({
            room_id: row.room_id,
            avg_watts: parseFloat(row.avg_watts) || 0,
            min_watts: row.min_watts || 0,
            max_watts: row.max_watts || 0,
            readings_count: parseInt(row.readings_count) || 0,
            total_watts: parseInt(row.total_watts) || 0
        }));
    }

    static async getHourlyUsageAverages(
        client: any,
        roomId: string,
        startDate: Date,
        endDate: Date
    ): Promise<Array<{ timestamp: Date; avg_watts: number }>> {
        const query = `
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                AVG(power_watts)::numeric(10,2) as avg_watts
            FROM ${this.tableName}
            WHERE room_id = $1 AND timestamp >= $2 AND timestamp <= $3
            GROUP BY DATE_TRUNC('hour', timestamp)
            ORDER BY hour
        `;
        
        const result = await client.query(query, [roomId, startDate, endDate]);
        
        return result.rows.map((row: any) => ({
            timestamp: row.hour,
            avg_watts: parseFloat(row.avg_watts) || 0
        }));
    }

    static async getLatestPowerReading(client: any, roomId: string): Promise<PowerUsage | null> {
        const query = `
            SELECT * FROM ${this.tableName}
            WHERE room_id = $1
            ORDER BY timestamp DESC
            LIMIT 1
        `;
        
        const result = await client.query(query, [roomId]);
        return result.rows[0] || null;
    }

    static async getAllRooms(client: any): Promise<string[]> {
        const query = `
            SELECT DISTINCT room_id 
            FROM ${this.tableName} 
            ORDER BY room_id
        `;
        
        const result = await client.query(query);
        return result.rows.map((row: any) => row.room_id);
    }

    static async getDailyUsageStats(
        client: any,
        startDate: Date,
        endDate: Date,
        roomIds?: string[]
    ): Promise<Array<{
        date: string;
        room_id: string;
        avg_watts: number;
        max_watts: number;
        min_watts: number;
        total_readings: number;
        kwh_consumed: number; // Calculated based on average watts over time
    }>> {
        let query = `
            SELECT 
                DATE(timestamp) as date,
                room_id,
                AVG(power_watts)::numeric(10,2) as avg_watts,
                MAX(power_watts) as max_watts,
                MIN(power_watts) as min_watts,
                COUNT(power_watts) as total_readings,
                -- Estimate kWh: (avg_watts * hours_in_day) / 1000
                (AVG(power_watts) * 24 / 1000)::numeric(10,3) as kwh_consumed
            FROM ${this.tableName}
            WHERE timestamp >= $1 AND timestamp <= $2
        `;
        
        const params: any[] = [startDate, endDate];
        
        if (roomIds && roomIds.length > 0) {
            query += ` AND room_id = ANY($3::text[])`;
            params.push(roomIds);
        }
        
        query += ` GROUP BY DATE(timestamp), room_id ORDER BY date DESC, room_id`;
        
        const result = await client.query(query, params);
        return result.rows.map((row: any) => ({
            date: row.date,
            room_id: row.room_id,
            avg_watts: parseFloat(row.avg_watts) || 0,
            max_watts: row.max_watts || 0,
            min_watts: row.min_watts || 0,
            total_readings: parseInt(row.total_readings) || 0,
            kwh_consumed: parseFloat(row.kwh_consumed) || 0
        }));
    }

    static async clearOldPowerUsageData(client: any, daysToKeep: number = 30): Promise<number> {
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
        
        const query = `
            DELETE FROM ${this.tableName}
            WHERE timestamp < $1
        `;
        
        const result = await client.query(query, [cutoffDate]);
        return result.rowCount || 0;
    }
}