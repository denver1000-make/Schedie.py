import express from 'express';
import * as mqtt from 'mqtt';
import path from 'path';
import { Database } from './database';
import { ProcessedScheduleIdsModel } from './models/ProcessedScheduleIds';
import { ResolvedScheduleSlotsModel } from './models/ResolvedScheduleSlots';
import { RunningTurnOnJobsModel } from './models/RunningTurnOnJobs';
import { CancelledSchedulesModel } from './models/CancelledSchedules';
import { ScheduleWrapperModel } from './models/ScheduleWrapper';

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
});

client.on('message', (topic, message) => {
    const messageStr = message.toString();
    console.log(`Received message on ${topic}: ${messageStr}`);
    
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

        // Create tables if they don't exist
        await Database.createTables();

        // Fetch processed schedules
        const client = await Database.getClient();
        
        try {
            // Get the most recent non-temporary schedule from schedule_wrappers
            const mostRecentPermanentWrapper = await ScheduleWrapperModel.findMostRecentPermanent(client);
            
            // Get all temporary schedules directly from resolved_schedule_slots (they don't have wrapper entries)
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = await client.query(temporarySchedulesQuery);
            
            let scheduleIds: string[] = [];
            
            // Add the most recent permanent schedule
            if (mostRecentPermanentWrapper) {
                scheduleIds.push(mostRecentPermanentWrapper.schedule_id);
                console.log('Added permanent schedule:', mostRecentPermanentWrapper.schedule_id);
            }
            
            // Add all temporary schedules (from resolved_schedule_slots directly)
            if (temporaryResult.rows.length > 0) {
                const tempIds = temporaryResult.rows.map((row: any) => row.schedule_id);
                scheduleIds.push(...tempIds);
                console.log('Added temporary schedules:', tempIds);
            }
            
            if (scheduleIds.length === 0) {
                console.log('No schedules found in either schedule_wrappers or resolved_schedule_slots');
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

// API endpoint to get bridge status
app.get('/api/bridge-status', (req, res) => {
    res.json(bridgeStatus);
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
            // Get the most recent non-temporary schedule from schedule_wrappers
            const mostRecentPermanentWrapper = await ScheduleWrapperModel.findMostRecentPermanent(client);
            
            // Get all temporary schedules directly from resolved_schedule_slots
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = await client.query(temporarySchedulesQuery);
            
            let scheduleIds: string[] = [];
            
            // Add the most recent permanent schedule
            if (mostRecentPermanentWrapper) {
                scheduleIds.push(mostRecentPermanentWrapper.schedule_id);
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
            // Get the most recent non-temporary schedule from schedule_wrappers
            const mostRecentPermanentWrapper = await ScheduleWrapperModel.findMostRecentPermanent(client);
            
            // Get all temporary schedules directly from resolved_schedule_slots
            const temporarySchedulesQuery = `
                SELECT DISTINCT schedule_id FROM resolved_schedule_slots 
                WHERE is_temporary = true
                ORDER BY schedule_id
            `;
            const temporaryResult = await client.query(temporarySchedulesQuery);
            
            let scheduleIds: string[] = [];
            
            // Add the most recent permanent schedule
            if (mostRecentPermanentWrapper) {
                scheduleIds.push(mostRecentPermanentWrapper.schedule_id);
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
                    mostRecentScheduleId: null,
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
                permanentScheduleId: mostRecentPermanentWrapper ? mostRecentPermanentWrapper.schedule_id : null,
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
                upload_date_epoch: BigInt(Date.now()),
                is_temporary: req.body.is_temporary || false,
                is_synced_to_remote: req.body.is_synced_to_remote !== undefined ? req.body.is_synced_to_remote : true,
                is_from_remote: req.body.is_from_remote !== undefined ? req.body.is_from_remote : true
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

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});