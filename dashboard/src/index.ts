import express from 'express';
import * as mqtt from 'mqtt';
import path from 'path';
import { Database } from './database';
import { ProcessedScheduleIdsModel } from './models/ProcessedScheduleIds';
import { ResolvedScheduleSlotsModel } from './models/ResolvedScheduleSlots';
import { RunningTurnOnJobsModel } from './models/RunningTurnOnJobs';
import { CancelledSchedulesModel } from './models/CancelledSchedules';
import { ScheduleWrapperModel } from './models/ScheduleWrapper';
import { PowerUsageModel } from './models/PowerUsage';

const app = express();
const port = 8080;

// Initialize Database
Database.initialize();

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
        lastUpdate: null as Date | null,
        state: 'unknown',
        connectionHistory: [] as Array<{timestamp: Date, state: string, message?: string}>
    }
};

// MQTT Debug Message Store
let mqttDebugMessages: Array<{
    timestamp: Date,
    topic: string,
    message: string,
    messageType: string
}> = [];

client.on('connect', () => {
    console.log('Connected to MQTT broker:', brokerUrl);
    
    // Subscribe to all bridge connection events to detect any bridge
    client.subscribe('$SYS/broker/connection/+/state', (err) => {
        if (!err) {
            console.log('Subscribed to all bridge connection states');
        } else {
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
        if (!err) console.log('Subscribed to connected clients topic');
    });
    
    // Subscribe to log messages if available (some brokers expose these)
    client.subscribe('$SYS/broker/log/+', (err) => {
        if (!err) console.log('Subscribed to broker logs');
    });
    
    // Subscribe to ESP32 power usage reports
    client.subscribe('usage_report/+', (err) => {
        if (!err) console.log('Subscribed to power usage reports from ESP32 devices');
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
    
    // Handle ESP32 power usage reports
    if (topic.startsWith('usage_report/')) {
        const roomId = topic.split('/')[1]; // Extract room ID from topic
        const powerWatts = parseInt(messageStr);
        
        if (!isNaN(powerWatts) && roomId) {
            // Store power usage data in database
            Database.getClient().then(async (client) => {
                try {
                    await PowerUsageModel.insertPowerUsage(client, {
                        timestamp: new Date(),
                        room_id: roomId,
                        power_watts: powerWatts
                    });
                    console.log(`📊 Stored power usage: ${roomId} = ${powerWatts}W`);
                } catch (error) {
                    console.error('❌ Failed to store power usage:', error);
                } finally {
                    client.release();
                }
            });
        }
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
app.set('views', path.join(__dirname, '../views'));

// Middleware
app.use(express.json());
app.use(express.static('public')); // Serve static files from public directory

// Routes
app.get('/', async (req, res) => {
    try {
        // Test database connection
        const isConnected = await Database.testConnection();
        
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
            await Database.createTables();
        } catch (error) {
            // Log but don't display database constraint errors to users
            console.log('Database setup completed with some warnings (constraints may already exist)');
        }

        // Fetch processed schedules
        const client = await Database.getClient();
        
        try {
            // Get the in_use permanent schedule from schedule_wrappers
            const inUsePermanentWrapper = await ScheduleWrapperModel.findMostRecentPermanent(client);
            
            // Get all temporary schedules from schedule_wrappers (active by existence)
            const temporaryWrappers = await ScheduleWrapperModel.findActiveTemporary(client);
            
            let scheduleIds: string[] = [];
            
            // Add the in_use permanent schedule
            if (inUsePermanentWrapper) {
                scheduleIds.push(inUsePermanentWrapper.schedule_id);
                console.log('Added in_use permanent schedule:', inUsePermanentWrapper.schedule_id);
            }
            
            // Add all temporary schedules (active by existence)
            if (temporaryWrappers.length > 0) {
                const tempIds = temporaryWrappers.map(wrapper => wrapper.schedule_id);
                scheduleIds.push(...tempIds);
                console.log('Added temporary schedules:', tempIds);
            }
            
            if (scheduleIds.length === 0) {
                console.log('No schedules found - checking if tables exist and have data');
                
                // Debug: Check if tables exist and have data
                const debugQuery1 = `SELECT COUNT(*) as count FROM schedule_wrappers_v2`;
                const debugQuery2 = `SELECT COUNT(*) as count FROM resolved_schedule_slots_v2`;
                
                try {
                    const wrapperCount = await client.query(debugQuery1);
                    const slotsCount = await client.query(debugQuery2);
                    console.log('Schedule wrappers_v2 count:', wrapperCount.rows[0].count);
                    console.log('Resolved slots count:', slotsCount.rows[0].count);
                } catch (debugError) {
                    console.log('Debug query error:', debugError);
                }
            } else {
                console.log('Consolidated schedule IDs:', scheduleIds);
            }

            const schedules = await ProcessedScheduleIdsModel.findAll(client);
            
            // Fetch enhanced weekly schedule using consolidated schedule_ids
            let enhancedData;
            if (scheduleIds.length > 0) {
                enhancedData = await ResolvedScheduleSlotsModel.getConsolidatedWeeklySchedule(client, scheduleIds);
            } else {
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
            const tempSlots = Object.values(enhancedData.schedules).reduce((sum, daySchedules) => 
                sum + daySchedules.filter(slot => slot.is_temporary).length, 0);
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
            const runningJobsObj: {[key: string]: any} = {};
            enhancedData.runningJobs.forEach((value, key) => {
                runningJobsObj[key] = value;
            });

            const cancelledSchedulesObj: {[key: string]: any[]} = {};
            enhancedData.cancelledSchedules.forEach((value, key) => {
                cancelledSchedulesObj[key] = value;
            });

            const roomGroupsObj: {[key: string]: any[]} = {};
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

        } finally {
            client.release();
        }

    } catch (error) {
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
});

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
    const limit = parseInt(req.query.limit as string) || 50;
    const messageType = req.query.type as string;
    
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
app.get('/api/schedules', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            const schedules = await ProcessedScheduleIdsModel.findAll(client);
            res.json(schedules);
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
});

// API endpoint to get schedule slots
app.get('/api/schedules/:id/slots', async (req, res) => {
    try {
        const { id } = req.params;
        const client = await Database.getClient();
        
        try {
            const slots = await ResolvedScheduleSlotsModel.findByScheduleId(client, id);
            res.json(slots);
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
});

// API endpoint to get weekly schedule
app.get('/api/weekly-schedule', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            // Get the in_use permanent schedule from schedule_wrappers
            const inUsePermanentWrapper = await ScheduleWrapperModel.findMostRecentPermanent(client);
            
            // Get all temporary schedules directly from resolved_schedule_slots
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = await client.query(temporarySchedulesQuery);
            
            let scheduleIds: string[] = [];
            
            // Add the in_use permanent schedule
            if (inUsePermanentWrapper) {
                scheduleIds.push(inUsePermanentWrapper.schedule_id);
            }
            
            // Add all temporary schedules
            if (temporaryResult.rows.length > 0) {
                const tempIds = temporaryResult.rows.map((row: any) => row.schedule_id);
                scheduleIds.push(...tempIds);
            }
            
            if (scheduleIds.length === 0) {
                // No schedules found - return empty schedule
                return res.json({
                    'Monday': [], 'Tuesday': [], 'Wednesday': [], 'Thursday': [], 'Friday': [], 'Saturday': []
                });
            }

            // Get consolidated weekly schedule
            const consolidatedData = await ResolvedScheduleSlotsModel.getConsolidatedWeeklySchedule(client, scheduleIds);
            res.json(consolidatedData.schedules);
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
});

// API endpoint to get weekly schedule for a specific room
app.get('/api/weekly-schedule/:roomId', async (req, res) => {
    try {
        const { roomId } = req.params;
        const client = await Database.getClient();
        
        try {
            const weeklySchedule = await ResolvedScheduleSlotsModel.getRoomWeeklySchedule(client, roomId);
            res.json(weeklySchedule);
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
});

// API endpoint to get latest schedule data with status
app.get('/api/schedule-status', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            // Get the in_use permanent schedule from schedule_wrappers
            const inUsePermanentWrapper = await ScheduleWrapperModel.findMostRecentPermanent(client);
            
            // Get all temporary schedules directly from resolved_schedule_slots
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = await client.query(temporarySchedulesQuery);
            
            let scheduleIds: string[] = [];
            
            // Add the in_use permanent schedule
            if (inUsePermanentWrapper) {
                scheduleIds.push(inUsePermanentWrapper.schedule_id);
            }
            
            // Add all temporary schedules
            if (temporaryResult.rows.length > 0) {
                const tempIds = temporaryResult.rows.map((row: any) => row.schedule_id);
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
            const enhancedData = await ResolvedScheduleSlotsModel.getConsolidatedWeeklySchedule(client, scheduleIds);
            
            // Convert Maps to objects for JSON response
            const runningJobsObj: {[key: string]: any} = {};
            enhancedData.runningJobs.forEach((value, key) => {
                runningJobsObj[key] = value;
            });

            const cancelledSchedulesObj: {[key: string]: any[]} = {};
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
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('API error:', error);
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        res.status(500).json({ error: errorMessage });
    }
});

// Test endpoint to create schedule wrapper data
app.post('/api/test/create-schedule-wrapper', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            // Create a test schedule wrapper entry
            const scheduleId = req.body.schedule_id || `test_schedule_${Date.now()}`;
            
            const wrapper = await ScheduleWrapperModel.create(client, {
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
        } finally {
            client.release();
        }
    } catch (error: any) {
        console.error('Test endpoint error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message || 'Internal server error' 
        });
    }
});

// Test endpoint to create running job
app.post('/api/test/create-running-job', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            const timeslotId = req.body.timeslot_id || `test_slot_${Date.now()}`;
            const isTemporary = req.body.is_temporary || false;
            
            const runningJob = await RunningTurnOnJobsModel.create(client, {
                timeslot_id: timeslotId,
                is_temporary: isTemporary
            });
            
            res.json({
                success: true,
                message: 'Running job created successfully',
                data: runningJob
            });
        } finally {
            client.release();
        }
    } catch (error: any) {
        console.error('Test endpoint error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message || 'Internal server error' 
        });
    }
});

// Test endpoint to create cancelled schedule
app.post('/api/test/create-cancelled-schedule', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            const timeslotId = req.body.timeslot_id || `test_slot_${Date.now()}`;
            const today = new Date().toISOString().split('T')[0];
            
            const cancelledSchedule = await CancelledSchedulesModel.create(client, {
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
        } finally {
            client.release();
        }
    } catch (error: any) {
        console.error('Test endpoint error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message || 'Internal server error' 
        });
    }
});

// Debug endpoint to check current data state
app.get('/api/debug/data-status', async (req, res) => {
    try {
        console.log('Debug endpoint called');
        const client = await Database.getClient();
        
        try {
            console.log('Getting schedule wrapper data...');
            // Get schedule wrapper data
            const allWrappers = await ScheduleWrapperModel.findAll(client);
            console.log('Found wrappers:', allWrappers.length);
            
            const mostRecentWrapper = await ScheduleWrapperModel.findMostRecent(client);
            console.log('Most recent wrapper:', mostRecentWrapper);
            
            // Get resolved schedule slots counts
            console.log('Getting resolved schedule slots...');
            const allSlotsQuery = 'SELECT COUNT(*) as total, schedule_id FROM resolved_schedule_slots GROUP BY schedule_id';
            const allSlotsResult = await client.query(allSlotsQuery);
            console.log('Slots result:', allSlotsResult.rows);
            
            // Get running jobs and cancelled schedules
            console.log('Getting running jobs and cancelled schedules...');
            const runningJobs = await RunningTurnOnJobsModel.findAll(client);
            const cancelledSchedules = await CancelledSchedulesModel.findAll(client);
            
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
        } finally {
            client.release();
        }
    } catch (error: any) {
        console.error('Debug endpoint error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message || 'Internal server error',
            stack: error.stack
        });
    }
});

// Keep the MQTT publish endpoint for potential future use
app.post('/publish', async (req, res) => {
    try {
        const { topic, message } = req.body;
        
        if (!client) {
            return res.status(500).json({ 
                success: false, 
                error: 'MQTT client not connected' 
            });
        }
        
        client.publish(topic, message, (err: any) => {
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
    } catch (error: any) {
        console.error('Publish error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Power Usage Routes
app.get('/power-usage', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            const selectedRoom = req.query.room as string || 'all';
            const startDate = req.query.startDate as string || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
            const endDate = req.query.endDate as string || new Date().toISOString().split('T')[0];
            const reportType = req.query.reportType as string || 'summary';
            
            // Get available rooms
            const availableRooms = await PowerUsageModel.getAllRooms(client);
            
            // Get usage data based on filters
            let usageData;
            let roomIds = selectedRoom === 'all' ? undefined : [selectedRoom];
            
            if (reportType === 'daily') {
                usageData = await PowerUsageModel.getDailyUsageStats(
                    client,
                    new Date(startDate),
                    new Date(endDate),
                    roomIds
                );
            } else {
                usageData = await PowerUsageModel.getPowerUsageSummaryByDateRange(
                    client,
                    new Date(startDate),
                    new Date(endDate),
                    roomIds
                );
            }
            
            // Calculate statistics
            const stats = {
                totalWatts: usageData.reduce((sum, data) => sum + (data.avg_watts || 0), 0),
                totalKwh: usageData.reduce((sum, data) => {
                    const kwh = (data as any).kwh_consumed || ((data as any).total_watts || 0) / 1000;
                    return sum + kwh;
                }, 0),
                avgWatts: usageData.length > 0 ? usageData.reduce((sum, data) => sum + (data.avg_watts || 0), 0) / usageData.length : 0,
                estimatedCost: 0,
                highUsageRooms: usageData.filter(data => (data.avg_watts || 0) > 1000)
            };
            
            stats.estimatedCost = stats.totalKwh * 12; // ₱12 per kWh
            
            // Get room stats for current power readings
            const roomStats: any = {};
            for (const room of availableRooms) {
                const latestReading = await PowerUsageModel.getLatestPowerReading(client, room);
                roomStats[room] = {
                    currentWatts: latestReading?.power_watts || 0
                };
            }
            
            // Prepare chart data
            const chartData = {
                timeLabels: usageData.map(d => (d as any).date || d.room_id),
                powerData: usageData.map(d => d.avg_watts || 0),
                roomLabels: availableRooms,
                roomData: availableRooms.map(room => {
                    const roomData = usageData.find(d => d.room_id === room);
                    return roomData?.avg_watts || 0;
                })
            };
            
            res.render('power-usage', {
                title: 'Power Usage Report',
                availableRooms,
                selectedRoom,
                startDate,
                endDate,
                reportType,
                usageData,
                stats,
                roomStats,
                chartData
            });
            
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('Power usage page error:', error);
        res.status(500).send('Internal Server Error');
    }
});

// Power Usage API endpoint
app.get('/api/power-usage', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            const selectedRoom = req.query.room as string;
            const startDate = req.query.startDate as string;
            const endDate = req.query.endDate as string;
            const limit = req.query.limit ? parseInt(req.query.limit as string) : undefined;
            
            let data;
            if (selectedRoom && selectedRoom !== 'all') {
                data = await PowerUsageModel.getPowerUsageByRoomAndDateRange(
                    client,
                    selectedRoom,
                    new Date(startDate),
                    new Date(endDate),
                    limit
                );
            } else {
                data = await PowerUsageModel.getPowerUsageSummaryByDateRange(
                    client,
                    new Date(startDate),
                    new Date(endDate)
                );
            }
            
            res.json({ success: true, data });
            
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('Power usage API error:', error);
        res.status(500).json({ success: false, error: (error as Error).message });
    }
});

// Power Usage Export endpoint
app.get('/power-usage/export', async (req, res) => {
    try {
        const client = await Database.getClient();
        
        try {
            const selectedRoom = req.query.room as string || 'all';
            const startDate = req.query.startDate as string || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
            const endDate = req.query.endDate as string || new Date().toISOString().split('T')[0];
            
            let roomIds = selectedRoom === 'all' ? undefined : [selectedRoom];
            
            const usageData = await PowerUsageModel.getDailyUsageStats(
                client,
                new Date(startDate),
                new Date(endDate),
                roomIds
            );
            
            // Generate CSV
            let csv = 'Date,Room ID,Avg Power (W),Max Power (W),Min Power (W),Energy (kWh),Readings,Est Cost (₱)\n';
            usageData.forEach(data => {
                csv += `${data.date},${data.room_id},${data.avg_watts},${data.max_watts},${data.min_watts},${data.kwh_consumed},${data.total_readings},${(data.kwh_consumed * 12).toFixed(2)}\n`;
            });
            
            res.setHeader('Content-Type', 'text/csv');
            res.setHeader('Content-Disposition', `attachment; filename=power-usage-${startDate}-to-${endDate}.csv`);
            res.send(csv);
            
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('Power usage export error:', error);
        res.status(500).send('Export failed');
    }
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});