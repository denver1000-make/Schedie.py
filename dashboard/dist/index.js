"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const pg_1 = require("pg");
const mqtt = __importStar(require("mqtt"));
const path = __importStar(require("path"));
const app = (0, express_1.default)();
const port = 8080;
// Set EJS as the template engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, '../views'));
// Database connection
const pool = new pg_1.Pool({
    user: 'postgres',
    password: 'postgres',
    host: 'postgres',
    port: 5432,
    database: 'schedule_db'
});
// MQTT client setup for power usage monitoring
const brokerUrl = 'mqtt://mosquitto_rpi:1883';
const mqttOptions = {
    username: 'denver',
    password: 'denver',
    port: 1883
};
const client = mqtt.connect(brokerUrl, mqttOptions);
// Store latest power readings
let latestPowerReadings = {};
client.on('connect', () => {
    console.log('Connected to MQTT broker for power monitoring');
    client.subscribe('usage_report/+', (err) => {
        if (!err)
            console.log('Subscribed to power usage reports');
    });
});
client.on('message', (topic, message) => __awaiter(void 0, void 0, void 0, function* () {
    if (topic.startsWith('usage_report/')) {
        const roomId = topic.split('/')[1];
        const powerWatts = parseInt(message.toString());
        if (!isNaN(powerWatts) && roomId) {
            // Store latest reading
            latestPowerReadings[roomId] = {
                watts: powerWatts,
                timestamp: new Date()
            };
            // Save to database
            try {
                const dbClient = yield pool.connect();
                yield dbClient.query('INSERT INTO power_usage (timestamp, room_id, power_watts) VALUES ($1, $2, $3)', [new Date(), roomId, powerWatts]);
                dbClient.release();
                console.log(`📊 Stored power usage: ${roomId} = ${powerWatts}W`);
            }
            catch (error) {
                console.error('Failed to store power usage:', error);
            }
        }
    }
}));
// Middleware for serving static files
app.use(express_1.default.static('public'));
// Schedule dashboard route
app.get('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield pool.connect();
        // Get query parameters for filtering
        const selectedRoom = req.query.room || 'all';
        const dateRange = req.query.dateRange || '24h';
        const sortBy = req.query.sortBy || 'timestamp';
        const sortOrder = req.query.sortOrder || 'desc';
        // Get in_use permanent schedule IDs
        const permanentWrappers = yield client.query('SELECT schedule_id FROM schedule_wrappers_v2 WHERE is_temporary = false AND in_use = true');
        // Get all temporary schedule IDs
        const temporaryWrappers = yield client.query('SELECT schedule_id FROM schedule_wrappers_v2 WHERE is_temporary = true');
        // Combine all schedule IDs
        const allScheduleIds = [
            ...permanentWrappers.rows.map(row => row.schedule_id),
            ...temporaryWrappers.rows.map(row => row.schedule_id)
        ];
        let allSlots = [];
        if (allScheduleIds.length > 0) {
            // Get all schedule slots for these schedule IDs
            const placeholders = allScheduleIds.map((_, index) => `$${index + 1}`).join(',');
            const slotsQuery = `
                SELECT 
                    timeslot_id,
                    schedule_id,
                    room_id,
                    day_name,
                    day_order,
                    start_time,
                    end_time,
                    subject,
                    teacher,
                    teacher_email,
                    is_temporary
                FROM resolved_schedule_slots_v2 
                WHERE schedule_id IN (${placeholders})
                ORDER BY day_order, start_time
            `;
            const slotsResult = yield client.query(slotsQuery, allScheduleIds);
            allSlots = slotsResult.rows;
        }
        // Group slots by day (handle case-insensitive day names)
        const dayOrder = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const scheduleByDay = {};
        dayOrder.forEach(day => {
            scheduleByDay[day] = allSlots.filter(slot => slot.day_name.toLowerCase() === day.toLowerCase());
        });
        // Get date range for power usage query
        let dateFilter = "timestamp >= NOW() - INTERVAL '1 hour'";
        let dateDescription = "Last Hour";
        switch (dateRange) {
            case '1h':
                dateFilter = "timestamp >= NOW() - INTERVAL '1 hour'";
                dateDescription = "Last Hour";
                break;
            case '24h':
                dateFilter = "timestamp >= NOW() - INTERVAL '24 hours'";
                dateDescription = "Last 24 Hours";
                break;
            case '7d':
                dateFilter = "timestamp >= NOW() - INTERVAL '7 days'";
                dateDescription = "Last 7 Days";
                break;
            case '30d':
                dateFilter = "timestamp >= NOW() - INTERVAL '30 days'";
                dateDescription = "Last 30 Days";
                break;
        }
        // Build room filter
        let roomFilter = "";
        let roomParams = [];
        if (selectedRoom !== 'all') {
            roomFilter = "AND room_id = $1";
            roomParams = [selectedRoom];
        }
        // Get available rooms for dropdown
        const roomsQuery = "SELECT DISTINCT room_id FROM power_usage ORDER BY room_id";
        const roomsResult = yield client.query(roomsQuery);
        const availableRooms = roomsResult.rows.map(row => row.room_id);
        // Get detailed power usage data with kWh calculation
        const detailedPowerQuery = `
            WITH power_with_intervals AS (
                SELECT 
                    room_id,
                    timestamp,
                    power_watts,
                    LAG(timestamp) OVER (PARTITION BY room_id ORDER BY timestamp) as prev_timestamp
                FROM power_usage 
                WHERE ${dateFilter} ${roomFilter}
            )
            SELECT 
                room_id,
                timestamp,
                power_watts,
                CASE 
                    WHEN prev_timestamp IS NOT NULL 
                    THEN (power_watts / 1000.0) * EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0
                    ELSE 0
                END as kwh_consumed
            FROM power_with_intervals
            ORDER BY ${sortBy === 'room' ? 'room_id' : sortBy === 'power' ? 'power_watts' : 'timestamp'} ${sortOrder}
            LIMIT 500
        `;
        const detailedPowerResult = yield client.query(detailedPowerQuery, roomParams);
        const detailedPowerData = detailedPowerResult.rows;
        // Get summary power usage data
        const summaryPowerQuery = `
            WITH power_with_intervals AS (
                SELECT 
                    room_id,
                    timestamp,
                    power_watts,
                    LAG(timestamp) OVER (PARTITION BY room_id ORDER BY timestamp) as prev_timestamp
                FROM power_usage 
                WHERE ${dateFilter} ${roomFilter}
            ),
            power_with_kwh AS (
                SELECT 
                    room_id,
                    timestamp,
                    power_watts,
                    CASE 
                        WHEN prev_timestamp IS NOT NULL 
                        THEN (power_watts / 1000.0) * EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0
                        ELSE 0
                    END as kwh_consumed
                FROM power_with_intervals
            )
            SELECT 
                room_id,
                AVG(power_watts) as avg_watts,
                MAX(power_watts) as max_watts,
                MIN(power_watts) as min_watts,
                COUNT(*) as reading_count,
                MAX(timestamp) as last_reading,
                SUM(kwh_consumed) as total_kwh
            FROM power_with_kwh
            GROUP BY room_id
            ORDER BY ${sortBy === 'room' ? 'room_id' : sortBy === 'kwh' ? 'total_kwh' : 'avg_watts'} ${sortOrder}
        `;
        const summaryPowerResult = yield client.query(summaryPowerQuery, roomParams);
        const summaryPowerData = summaryPowerResult.rows;
        // Prepare stats
        const stats = {
            permanentSchedules: permanentWrappers.rows.length,
            temporarySchedules: temporaryWrappers.rows.length,
            totalSlots: allSlots.length,
            activePowerMonitors: summaryPowerData.length,
            totalKwh: summaryPowerData.reduce((sum, room) => sum + (parseFloat(room.total_kwh) || 0), 0),
            avgPowerConsumption: summaryPowerData.length > 0 ?
                summaryPowerData.reduce((sum, room) => sum + (parseFloat(room.avg_watts) || 0), 0) / summaryPowerData.length : 0
        };
        client.release();
        // Render the schedule dashboard template
        res.render('schedule-dashboard', {
            stats: {
                permanentSchedules: permanentWrappers.rows.length,
                temporarySchedules: temporaryWrappers.rows.length,
                totalSlots: allSlots.length
            },
            scheduleByDay,
            latestPowerReadings
        });
    }
    catch (error) {
        console.error('Database error:', error);
        res.send(`<h1>Error</h1><p>${error}</p>`);
    }
}));
// Power usage dashboard route
app.get('/power-usage', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield pool.connect();
        // Get query parameters for filtering
        const selectedRoom = req.query.room || 'all';
        const startDate = req.query.startDate || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        const endDate = req.query.endDate || new Date().toISOString().split('T')[0];
        const sortBy = req.query.sortBy || 'timestamp';
        const sortOrder = req.query.sortOrder || 'desc';
        // Build date range filter
        const dateFilter = "timestamp >= $1 AND timestamp <= $2";
        let roomFilter = "";
        let queryParams = [startDate + ' 00:00:00', endDate + ' 23:59:59'];
        // Build room filter
        if (selectedRoom !== 'all') {
            roomFilter = "AND room_id = $3";
            queryParams.push(selectedRoom);
        }
        // Get available rooms for dropdown
        const roomsQuery = "SELECT DISTINCT room_id FROM power_usage ORDER BY room_id";
        const roomsResult = yield client.query(roomsQuery);
        const availableRooms = roomsResult.rows.map(row => row.room_id);
        // Get detailed power usage data with kWh calculation
        const detailedPowerQuery = `
            WITH power_with_intervals AS (
                SELECT 
                    room_id,
                    timestamp,
                    power_watts,
                    LAG(timestamp) OVER (PARTITION BY room_id ORDER BY timestamp) as prev_timestamp
                FROM power_usage 
                WHERE ${dateFilter} ${roomFilter}
            )
            SELECT 
                room_id,
                timestamp,
                power_watts,
                CASE 
                    WHEN prev_timestamp IS NOT NULL 
                    THEN (power_watts / 1000.0) * EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0
                    ELSE 0
                END as kwh_consumed
            FROM power_with_intervals
            ORDER BY ${sortBy === 'room' ? 'room_id' : sortBy === 'power' ? 'power_watts' : 'timestamp'} ${sortOrder}
            LIMIT 1000
        `;
        const detailedPowerResult = yield client.query(detailedPowerQuery, queryParams);
        const detailedPowerData = detailedPowerResult.rows;
        // Get summary power usage data per room
        const summaryPowerQuery = `
            WITH power_with_intervals AS (
                SELECT 
                    room_id,
                    timestamp,
                    power_watts,
                    LAG(timestamp) OVER (PARTITION BY room_id ORDER BY timestamp) as prev_timestamp
                FROM power_usage 
                WHERE ${dateFilter} ${roomFilter}
            ),
            power_with_kwh AS (
                SELECT 
                    room_id,
                    timestamp,
                    power_watts,
                    CASE 
                        WHEN prev_timestamp IS NOT NULL 
                        THEN (power_watts / 1000.0) * EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0
                        ELSE 0
                    END as kwh_consumed
                FROM power_with_intervals
            )
            SELECT 
                room_id,
                AVG(power_watts) as avg_watts,
                MAX(power_watts) as max_watts,
                MIN(power_watts) as min_watts,
                COUNT(*) as reading_count,
                MIN(timestamp) as first_reading,
                MAX(timestamp) as last_reading,
                SUM(kwh_consumed) as total_kwh
            FROM power_with_kwh
            GROUP BY room_id
            ORDER BY ${sortBy === 'room' ? 'room_id' : sortBy === 'kwh' ? 'total_kwh DESC' : 'avg_watts DESC'}
        `;
        const summaryPowerResult = yield client.query(summaryPowerQuery, queryParams);
        const summaryPowerData = summaryPowerResult.rows;
        // Calculate overall stats
        const stats = {
            totalRooms: summaryPowerData.length,
            totalReadings: summaryPowerData.reduce((sum, room) => sum + parseInt(room.reading_count), 0),
            totalKwh: summaryPowerData.reduce((sum, room) => sum + (parseFloat(room.total_kwh) || 0), 0),
            avgPowerConsumption: summaryPowerData.length > 0 ?
                summaryPowerData.reduce((sum, room) => sum + (parseFloat(room.avg_watts) || 0), 0) / summaryPowerData.length : 0,
            estimatedCost: 0
        };
        stats.estimatedCost = stats.totalKwh * 12; // ₱12 per kWh
        client.release();
        // Render the power usage template
        res.render('power-usage', {
            stats,
            summaryPowerData,
            detailedPowerData,
            latestPowerReadings,
            availableRooms,
            selectedRoom,
            startDate,
            endDate,
            sortBy,
            sortOrder,
            dateRange: `${startDate} to ${endDate}`
        });
    }
    catch (error) {
        console.error('Power usage error:', error);
        res.send(`<h1>Error</h1><p>${error}</p>`);
    }
}));
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});
