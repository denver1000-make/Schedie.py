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
const mqtt = __importStar(require("mqtt"));
const path_1 = __importDefault(require("path"));
const database_1 = require("./database");
const ProcessedScheduleIds_1 = require("./models/ProcessedScheduleIds");
const ResolvedScheduleSlots_1 = require("./models/ResolvedScheduleSlots");
const RunningTurnOnJobs_1 = require("./models/RunningTurnOnJobs");
const CancelledSchedules_1 = require("./models/CancelledSchedules");
const ScheduleWrapper_1 = require("./models/ScheduleWrapper");
const app = (0, express_1.default)();
const port = 8080;
// Initialize Database
database_1.Database.initialize();
// MQTT client setup (keeping for potential future use)
const brokerUrl = 'mqtt://mosquitto_rpi:1883';
const mqttOptions = {
    username: 'denver',
    password: 'denver',
    port: 1883
};
const client = mqtt.connect(brokerUrl, mqttOptions);
// Bridge connection status tracking
let bridgeStatus = {
    mosquitto_local_test: {
        connected: false,
        lastUpdate: null,
        state: 'unknown',
        connectionHistory: []
    }
};
// MQTT Debug Message Store
let mqttDebugMessages = [];
client.on('connect', () => {
    console.log('Connected to MQTT broker:', brokerUrl);
    // Subscribe to all bridge connection events to detect any bridge
    client.subscribe('$SYS/broker/connection/+/state', (err) => {
        if (!err) {
            console.log('Subscribed to all bridge connection states');
        }
        else {
            console.error('Failed to subscribe to bridge connection states:', err);
        }
    });
    // Subscribe to bridge-specific topics if they exist
    client.subscribe('$SYS/broker/connection/mosquitto_local_test/state', (err) => {
        if (!err) {
            console.log('Subscribed to mosquitto_local_test bridge status');
        }
    });
    // Subscribe to general system topics for more info
    client.subscribe('$SYS/broker/clients/connected', (err) => {
        if (!err)
            console.log('Subscribed to connected clients topic');
    });
    // Subscribe to log messages if available (some brokers expose these)
    client.subscribe('$SYS/broker/log/+', (err) => {
        if (!err)
            console.log('Subscribed to broker logs');
    });
});
client.on('message', (topic, message) => {
    const messageStr = message.toString();
    console.log(`Received message on ${topic}: ${messageStr}`);
    // Store all MQTT messages for debug view
    const messageType = topic.startsWith('$SYS/broker/connection/') ? 'connection' :
        topic.startsWith('$SYS/broker/log/') ? 'log' :
            topic.startsWith('$SYS/broker/clients/') ? 'clients' :
                'other';
    mqttDebugMessages.unshift({
        timestamp: new Date(),
        topic: topic,
        message: messageStr,
        messageType: messageType
    });
    // Keep only last 100 messages
    if (mqttDebugMessages.length > 100) {
        mqttDebugMessages = mqttDebugMessages.slice(0, 100);
    }
    // Handle all bridge connections
    if (topic.startsWith('$SYS/broker/connection/') && topic.endsWith('/state')) {
        const connectionName = topic.split('/')[3];
        console.log(`Bridge connection ${connectionName} state: ${messageStr}`);
        const isConnected = messageStr === '1' || messageStr === 'connected';
        const state = messageStr === '1' ? 'connected' :
            messageStr === '0' ? 'disconnected' : messageStr;
        // Update bridge status (treat any bridge connection as the mosquitto_local_test bridge)
        // or specifically handle mosquitto_local_test
        if (connectionName.includes('mosquitto_local_test') || connectionName === 'remote_id') {
            const newStatus = {
                connected: isConnected,
                lastUpdate: new Date(),
                state: state,
                connectionHistory: [...(bridgeStatus.mosquitto_local_test.connectionHistory || [])]
            };
            // Add to connection history (keep last 10 entries)
            newStatus.connectionHistory.unshift({
                timestamp: new Date(),
                state: state,
                message: `Bridge ${connectionName} state: ${state} (raw: ${messageStr})`
            });
            if (newStatus.connectionHistory.length > 10) {
                newStatus.connectionHistory = newStatus.connectionHistory.slice(0, 10);
            }
            bridgeStatus.mosquitto_local_test = newStatus;
            console.log('Bridge status updated:', bridgeStatus.mosquitto_local_test);
        }
    }
    // Handle specific mosquitto_local_test bridge status
    if (topic.includes('mosquitto_local_test/state')) {
        const isConnected = messageStr === '1' || messageStr === 'connected';
        const state = messageStr === '1' ? 'connected' :
            messageStr === '0' ? 'disconnected' : messageStr;
        const newStatus = {
            connected: isConnected,
            lastUpdate: new Date(),
            state: state,
            connectionHistory: [...(bridgeStatus.mosquitto_local_test.connectionHistory || [])]
        };
        // Add to connection history (keep last 10 entries)
        newStatus.connectionHistory.unshift({
            timestamp: new Date(),
            state: state,
            message: `Direct bridge state changed to: ${state} (raw: ${messageStr})`
        });
        if (newStatus.connectionHistory.length > 10) {
            newStatus.connectionHistory = newStatus.connectionHistory.slice(0, 10);
        }
        bridgeStatus.mosquitto_local_test = newStatus;
        console.log('Direct bridge status updated:', bridgeStatus.mosquitto_local_test);
    }
    // Handle broker log messages
    if (topic.startsWith('$SYS/broker/log/')) {
        console.log(`Broker log: ${messageStr}`);
        // If it's related to our bridge, add it to history
        if (messageStr.includes('mosquitto_local_test') || messageStr.includes('bridge')) {
            if (!bridgeStatus.mosquitto_local_test.connectionHistory) {
                bridgeStatus.mosquitto_local_test.connectionHistory = [];
            }
            bridgeStatus.mosquitto_local_test.connectionHistory.unshift({
                timestamp: new Date(),
                state: bridgeStatus.mosquitto_local_test.state,
                message: messageStr
            });
            if (bridgeStatus.mosquitto_local_test.connectionHistory.length > 10) {
                bridgeStatus.mosquitto_local_test.connectionHistory =
                    bridgeStatus.mosquitto_local_test.connectionHistory.slice(0, 10);
            }
        }
    }
});
client.on('error', (error) => {
    console.error('MQTT connection error:', error);
});
// Set EJS as the template engine
app.set('view engine', 'ejs');
app.set('views', path_1.default.join(__dirname, '../views'));
// Middleware
app.use(express_1.default.json());
app.use(express_1.default.static('public')); // Serve static files from public directory
// Routes
app.get('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // Test database connection
        const isConnected = yield database_1.Database.testConnection();
        if (!isConnected) {
            return res.render('dashboard', {
                schedules: [],
                weeklySchedule: {
                    'Monday': [], 'Tuesday': [], 'Wednesday': [], 'Thursday': [], 'Friday': [], 'Saturday': []
                },
                runningJobs: {},
                cancelledSchedules: {},
                roomGroups: {},
                scheduleStats: {
                    totalSchedules: 0,
                    totalSlots: 0,
                    tempSchedules: 0,
                    runningJobs: 0,
                    cancelledSlots: 0
                },
                dbInfo: 'postgres@postgres:5432/schedule_db (Connection Failed)'
            });
        }
        // Create tables if they don't exist (suppress constraint errors)
        try {
            yield database_1.Database.createTables();
        }
        catch (error) {
            // Log but don't display database constraint errors to users
            console.log('Database setup completed with some warnings (constraints may already exist)');
        }
        // Fetch processed schedules
        const client = yield database_1.Database.getClient();
        try {
            // Get the in_use permanent schedule from schedule_wrappers
            const inUsePermanentWrapper = yield ScheduleWrapper_1.ScheduleWrapperModel.findMostRecentPermanent(client);
            // Get all temporary schedules directly from resolved_schedule_slots_v2 (regardless of in_use)
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots_v2 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = yield client.query(temporarySchedulesQuery);
            let scheduleIds = [];
            // Add the in_use permanent schedule
            if (inUsePermanentWrapper) {
                scheduleIds.push(inUsePermanentWrapper.schedule_id);
                console.log('Added in_use permanent schedule:', inUsePermanentWrapper.schedule_id);
            }
            // Add all temporary schedules (from resolved_schedule_slots_v2 directly)
            if (temporaryResult.rows.length > 0) {
                const tempIds = temporaryResult.rows.map((row) => row.schedule_id);
                scheduleIds.push(...tempIds);
                console.log('Added temporary schedules:', tempIds);
            }
            if (scheduleIds.length === 0) {
                console.log('No schedules found - checking if tables exist and have data');
                // Debug: Check if tables exist and have data
                const debugQuery1 = `SELECT COUNT(*) as count FROM schedule_wrappers`;
                const debugQuery2 = `SELECT COUNT(*) as count FROM resolved_schedule_slots_v2`;
                try {
                    const wrapperCount = yield client.query(debugQuery1);
                    const slotsCount = yield client.query(debugQuery2);
                    console.log('Schedule wrappers count:', wrapperCount.rows[0].count);
                    console.log('Resolved slots count:', slotsCount.rows[0].count);
                }
                catch (debugError) {
                    console.log('Debug query error:', debugError);
                }
            }
            else {
                console.log('Consolidated schedule IDs:', scheduleIds);
            }
            const schedules = yield ProcessedScheduleIds_1.ProcessedScheduleIdsModel.findAll(client);
            // Fetch enhanced weekly schedule using consolidated schedule_ids
            let enhancedData;
            if (scheduleIds.length > 0) {
                enhancedData = yield ResolvedScheduleSlots_1.ResolvedScheduleSlotsModel.getConsolidatedWeeklySchedule(client, scheduleIds);
            }
            else {
                // No schedule wrappers found - return empty data structure
                enhancedData = {
                    schedules: {
                        'Monday': [], 'Tuesday': [], 'Wednesday': [], 'Thursday': [], 'Friday': [], 'Saturday': []
                    },
                    runningJobs: new Map(),
                    cancelledSchedules: new Map(),
                    roomGroups: new Map()
                };
            }
            // Calculate statistics
            const totalSlots = Object.values(enhancedData.schedules).reduce((sum, daySchedules) => sum + daySchedules.length, 0);
            const tempSchedules = schedules.filter(s => s.is_temp).length;
            const tempSlots = Object.values(enhancedData.schedules).reduce((sum, daySchedules) => sum + daySchedules.filter(slot => slot.is_temporary).length, 0);
            const runningJobs = enhancedData.runningJobs.size;
            const cancelledSlots = enhancedData.cancelledSchedules.size;
            const scheduleStats = {
                totalSchedules: schedules.length,
                totalSlots: totalSlots,
                tempSchedules: tempSchedules,
                tempSlots: tempSlots,
                runningJobs: runningJobs,
                cancelledSlots: cancelledSlots
            };
            // Convert Maps to Objects for EJS template
            const runningJobsObj = {};
            enhancedData.runningJobs.forEach((value, key) => {
                runningJobsObj[key] = value;
            });
            const cancelledSchedulesObj = {};
            enhancedData.cancelledSchedules.forEach((value, key) => {
                cancelledSchedulesObj[key] = value;
            });
            const roomGroupsObj = {};
            enhancedData.roomGroups.forEach((value, key) => {
                roomGroupsObj[key] = value;
            });
            res.render('dashboard', {
                schedules,
                weeklySchedule: enhancedData.schedules,
                runningJobs: runningJobsObj,
                cancelledSchedules: cancelledSchedulesObj,
                roomGroups: roomGroupsObj,
                scheduleStats,
                dbInfo: 'postgres@postgres:5432/schedule_db (Connected)'
            });
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('Database error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.render('dashboard', {
            schedules: [],
            weeklySchedule: {
                'Monday': [], 'Tuesday': [], 'Wednesday': [], 'Thursday': [], 'Friday': [], 'Saturday': []
            },
            runningJobs: {},
            cancelledSchedules: {},
            roomGroups: {},
            scheduleStats: {
                totalSchedules: 0,
                totalSlots: 0,
                tempSchedules: 0,
                runningJobs: 0,
                cancelledSlots: 0
            },
            dbInfo: 'postgres@postgres:5432/schedule_db (Error: ' + errorMessage + ')'
        });
    }
}));
// Debug route for MQTT messages
app.get('/debug/mqtt', (req, res) => {
    res.render('mqtt-debug', {
        title: 'MQTT Debug Messages',
        bridgeStatus: bridgeStatus,
        dbInfo: 'postgres@postgres:5432/schedule_db'
    });
});
// API endpoint to get bridge status
app.get('/api/bridge-status', (req, res) => {
    res.json(bridgeStatus);
});
// API endpoint to get MQTT debug messages
app.get('/api/mqtt-debug', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    const messageType = req.query.type;
    let filteredMessages = mqttDebugMessages;
    if (messageType && messageType !== 'all') {
        filteredMessages = mqttDebugMessages.filter(msg => msg.messageType === messageType);
    }
    res.json({
        messages: filteredMessages.slice(0, limit),
        totalCount: mqttDebugMessages.length,
        messageTypes: ['all', 'connection', 'log', 'clients', 'other'],
        bridgeStatus: bridgeStatus
    });
});
// API endpoint to get schedules as JSON
app.get('/api/schedules', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield database_1.Database.getClient();
        try {
            const schedules = yield ProcessedScheduleIds_1.ProcessedScheduleIdsModel.findAll(client);
            res.json(schedules);
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
}));
// API endpoint to get schedule slots
app.get('/api/schedules/:id/slots', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const { id } = req.params;
        const client = yield database_1.Database.getClient();
        try {
            const slots = yield ResolvedScheduleSlots_1.ResolvedScheduleSlotsModel.findByScheduleId(client, id);
            res.json(slots);
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
}));
// API endpoint to get weekly schedule
app.get('/api/weekly-schedule', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield database_1.Database.getClient();
        try {
            // Get the in_use permanent schedule from schedule_wrappers
            const inUsePermanentWrapper = yield ScheduleWrapper_1.ScheduleWrapperModel.findMostRecentPermanent(client);
            // Get all temporary schedules directly from resolved_schedule_slots_v2
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots_v2 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = yield client.query(temporarySchedulesQuery);
            let scheduleIds = [];
            // Add the in_use permanent schedule
            if (inUsePermanentWrapper) {
                scheduleIds.push(inUsePermanentWrapper.schedule_id);
            }
            // Add all temporary schedules
            if (temporaryResult.rows.length > 0) {
                const tempIds = temporaryResult.rows.map((row) => row.schedule_id);
                scheduleIds.push(...tempIds);
            }
            if (scheduleIds.length === 0) {
                // No schedules found - return empty schedule
                return res.json({
                    'Monday': [], 'Tuesday': [], 'Wednesday': [], 'Thursday': [], 'Friday': [], 'Saturday': []
                });
            }
            // Get consolidated weekly schedule
            const consolidatedData = yield ResolvedScheduleSlots_1.ResolvedScheduleSlotsModel.getConsolidatedWeeklySchedule(client, scheduleIds);
            res.json(consolidatedData.schedules);
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
}));
// API endpoint to get weekly schedule for a specific room
app.get('/api/weekly-schedule/:roomId', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const { roomId } = req.params;
        const client = yield database_1.Database.getClient();
        try {
            const weeklySchedule = yield ResolvedScheduleSlots_1.ResolvedScheduleSlotsModel.getRoomWeeklySchedule(client, roomId);
            res.json(weeklySchedule);
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
}));
// API endpoint to get latest schedule data with status
app.get('/api/schedule-status', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield database_1.Database.getClient();
        try {
            // Get the in_use permanent schedule from schedule_wrappers
            const inUsePermanentWrapper = yield ScheduleWrapper_1.ScheduleWrapperModel.findMostRecentPermanent(client);
            // Get all temporary schedules directly from resolved_schedule_slots_v2
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots_v2 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = yield client.query(temporarySchedulesQuery);
            let scheduleIds = [];
            // Add the in_use permanent schedule
            if (inUsePermanentWrapper) {
                scheduleIds.push(inUsePermanentWrapper.schedule_id);
            }
            // Add all temporary schedules
            if (temporaryResult.rows.length > 0) {
                const tempIds = temporaryResult.rows.map((row) => row.schedule_id);
                scheduleIds.push(...tempIds);
            }
            if (scheduleIds.length === 0) {
                // No schedules found - return empty data
                return res.json({
                    weeklySchedule: {
                        'Monday': [], 'Tuesday': [], 'Wednesday': [], 'Thursday': [], 'Friday': [], 'Saturday': []
                    },
                    runningJobs: {},
                    cancelledSchedules: {},
                    consolidatedScheduleIds: [],
                    permanentScheduleId: null,
                    lastUpdate: new Date().toISOString(),
                    message: 'No schedules found'
                });
            }
            // Get consolidated data with status using all schedule_ids
            const enhancedData = yield ResolvedScheduleSlots_1.ResolvedScheduleSlotsModel.getConsolidatedWeeklySchedule(client, scheduleIds);
            // Convert Maps to objects for JSON response
            const runningJobsObj = {};
            enhancedData.runningJobs.forEach((value, key) => {
                runningJobsObj[key] = value;
            });
            const cancelledSchedulesObj = {};
            enhancedData.cancelledSchedules.forEach((value, key) => {
                cancelledSchedulesObj[key] = value;
            });
            res.json({
                weeklySchedule: enhancedData.schedules,
                runningJobs: runningJobsObj,
                cancelledSchedules: cancelledSchedulesObj,
                consolidatedScheduleIds: scheduleIds,
                permanentScheduleId: inUsePermanentWrapper ? inUsePermanentWrapper.schedule_id : null,
                lastUpdate: new Date().toISOString()
            });
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
}));
// Test endpoint to create schedule wrapper data
app.post('/api/test/create-schedule-wrapper', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield database_1.Database.getClient();
        try {
            // Create a test schedule wrapper entry
            const scheduleId = req.body.schedule_id || `test_schedule_${Date.now()}`;
            const wrapper = yield ScheduleWrapper_1.ScheduleWrapperModel.create(client, {
                schedule_id: scheduleId,
                upload_date_epoch: Date.now(),
                is_temporary: req.body.is_temporary || false,
                is_synced_to_remote: req.body.is_synced_to_remote !== undefined ? req.body.is_synced_to_remote : true,
                is_from_remote: req.body.is_from_remote !== undefined ? req.body.is_from_remote : true,
                in_use: req.body.in_use !== undefined ? req.body.in_use : true
            });
            res.json({
                success: true,
                message: 'Schedule wrapper created successfully',
                data: wrapper
            });
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('Test endpoint error:', error);
        res.status(500).json({
            success: false,
            error: error.message || 'Internal server error'
        });
    }
}));
// Test endpoint to create running job
app.post('/api/test/create-running-job', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield database_1.Database.getClient();
        try {
            const timeslotId = req.body.timeslot_id || `test_slot_${Date.now()}`;
            const isTemporary = req.body.is_temporary || false;
            const runningJob = yield RunningTurnOnJobs_1.RunningTurnOnJobsModel.create(client, {
                timeslot_id: timeslotId,
                is_temporary: isTemporary
            });
            res.json({
                success: true,
                message: 'Running job created successfully',
                data: runningJob
            });
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('Test endpoint error:', error);
        res.status(500).json({
            success: false,
            error: error.message || 'Internal server error'
        });
    }
}));
// Test endpoint to create cancelled schedule
app.post('/api/test/create-cancelled-schedule', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const client = yield database_1.Database.getClient();
        try {
            const timeslotId = req.body.timeslot_id || `test_slot_${Date.now()}`;
            const today = new Date().toISOString().split('T')[0];
            const cancelledSchedule = yield CancelledSchedules_1.CancelledSchedulesModel.create(client, {
                timeslot_id: timeslotId,
                cancellation_type: req.body.cancellation_type || 'manual',
                cancelled_date: req.body.cancelled_date || today,
                reason: req.body.reason || 'Test cancellation',
                cancelled_by: req.body.cancelled_by || 'test_user'
            });
            res.json({
                success: true,
                message: 'Cancelled schedule created successfully',
                data: cancelledSchedule
            });
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('Test endpoint error:', error);
        res.status(500).json({
            success: false,
            error: error.message || 'Internal server error'
        });
    }
}));
// Debug endpoint to check current data state
app.get('/api/debug/data-status', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        console.log('Debug endpoint called');
        const client = yield database_1.Database.getClient();
        try {
            console.log('Getting schedule wrapper data...');
            // Get schedule wrapper data
            const allWrappers = yield ScheduleWrapper_1.ScheduleWrapperModel.findAll(client);
            console.log('Found wrappers:', allWrappers.length);
            const mostRecentWrapper = yield ScheduleWrapper_1.ScheduleWrapperModel.findMostRecent(client);
            console.log('Most recent wrapper:', mostRecentWrapper);
            // Get resolved schedule slots counts
            console.log('Getting resolved schedule slots...');
            const allSlotsQuery = 'SELECT COUNT(*) as total, schedule_id FROM resolved_schedule_slots_v2 GROUP BY schedule_id';
            const allSlotsResult = yield client.query(allSlotsQuery);
            console.log('Slots result:', allSlotsResult.rows);
            // Get running jobs and cancelled schedules
            console.log('Getting running jobs and cancelled schedules...');
            const runningJobs = yield RunningTurnOnJobs_1.RunningTurnOnJobsModel.findAll(client);
            const cancelledSchedules = yield CancelledSchedules_1.CancelledSchedulesModel.findAll(client);
            const response = {
                schedule_wrappers: {
                    total: allWrappers.length,
                    most_recent: mostRecentWrapper,
                    all: allWrappers
                },
                resolved_schedule_slots: {
                    by_schedule_id: allSlotsResult.rows,
                    currently_using: mostRecentWrapper ? mostRecentWrapper.schedule_id : null
                },
                running_jobs: {
                    total: runningJobs.length,
                    jobs: runningJobs
                },
                cancelled_schedules: {
                    total: cancelledSchedules.length,
                    schedules: cancelledSchedules
                },
                message: mostRecentWrapper
                    ? `Currently showing schedule: ${mostRecentWrapper.schedule_id}`
                    : 'No active schedule wrapper found - dashboard will be empty'
            };
            console.log('Sending response...');
            res.json(response);
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('Debug endpoint error:', error);
        res.status(500).json({
            success: false,
            error: error.message || 'Internal server error',
            stack: error.stack
        });
    }
}));
// Keep the MQTT publish endpoint for potential future use
app.post('/publish', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const { topic, message } = req.body;
        if (!client) {
            return res.status(500).json({
                success: false,
                error: 'MQTT client not connected'
            });
        }
        client.publish(topic, message, (err) => {
            if (err) {
                console.error('MQTT publish error:', err);
                return res.status(500).json({
                    success: false,
                    error: err.message
                });
            }
            console.log(`Published to topic "${topic}": ${message}`);
            res.json({
                success: true,
                topic,
                message
            });
        });
    }
    catch (error) {
        console.error('Publish error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
}));
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});
