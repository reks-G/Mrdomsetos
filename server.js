const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ============ DATABASE ============
const DATABASE_URL = process.env.DATABASE_URL;
let pool = null;
let useDB = false;

if (DATABASE_URL) {
  try {
    const { Pool } = require('pg');
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    });
    useDB = true;
    console.log('Using PostgreSQL database');
    initDB();
  } catch (e) {
    console.log('PostgreSQL not available, using JSON files');
  }
} else {
  console.log('No DATABASE_URL, using JSON files');
}

async function initDB() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        email VARCHAR(255) PRIMARY KEY,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS servers (
        id VARCHAR(255) PRIMARY KEY,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS friends (
        id VARCHAR(255) PRIMARY KEY,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS dm_history (
        id VARCHAR(255) PRIMARY KEY,
        data JSONB NOT NULL
      );
    `);
    console.log('Database tables initialized');
    await loadFromDB();
  } catch (e) {
    console.error('DB init error:', e.message);
    useDB = false;
  }
}

async function loadFromDB() {
  if (!pool) return;
  try {
    // Load accounts
    const accRes = await pool.query('SELECT * FROM accounts');
    accRes.rows.forEach(row => accounts.set(row.email, row.data));
    
    // Load servers
    const srvRes = await pool.query('SELECT * FROM servers');
    srvRes.rows.forEach(row => {
      const srv = row.data;
      servers.set(row.id, {
        ...srv,
        members: new Set(srv.members || []),
        bans: new Set(srv.bans || [])
      });
    });
    
    // Load friends
    const frRes = await pool.query('SELECT * FROM friends');
    frRes.rows.forEach(row => {
      const d = row.data;
      if (d.friends) friends.set(row.id, new Set(d.friends));
      if (d.requests) friendRequests.set(row.id, new Set(d.requests));
      if (d.blocked) blockedUsers.set(row.id, new Set(d.blocked));
    });
    
    // Load DM history
    const dmRes = await pool.query('SELECT * FROM dm_history');
    dmRes.rows.forEach(row => dmHistory.set(row.id, row.data));
    
    console.log('Loaded from DB:', accounts.size, 'accounts,', servers.size, 'servers');
  } catch (e) {
    console.error('DB load error:', e.message);
  }
}

async function saveToDB() {
  if (!pool || !useDB) return;
  try {
    console.log('Saving to DB: accounts:', accounts.size, 'servers:', servers.size, 'friends:', friends.size);
    
    // Save accounts
    for (const [email, data] of accounts) {
      await pool.query(
        'INSERT INTO accounts (email, data) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET data = $2',
        [email, data]
      );
    }
    
    // Save servers
    for (const [id, srv] of servers) {
      const data = { ...srv, members: [...srv.members], bans: [...(srv.bans || [])] };
      await pool.query(
        'INSERT INTO servers (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2',
        [id, data]
      );
    }
    
    // Save friends
    const allUserIds = new Set([...friends.keys(), ...friendRequests.keys(), ...blockedUsers.keys()]);
    for (const userId of allUserIds) {
      const data = {
        friends: friends.has(userId) ? [...friends.get(userId)] : [],
        requests: friendRequests.has(userId) ? [...friendRequests.get(userId)] : [],
        blocked: blockedUsers.has(userId) ? [...blockedUsers.get(userId)] : []
      };
      await pool.query(
        'INSERT INTO friends (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2',
        [userId, data]
      );
    }
    
    // Save DM history
    for (const [key, msgs] of dmHistory) {
      await pool.query(
        'INSERT INTO dm_history (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2',
        [key, msgs]
      );
    }
    
    console.log('Saved to DB successfully');
  } catch (e) {
    console.error('DB save error:', e.message);
  }
}

// ============ JSON FALLBACK ============
const DATA_DIR = path.join(__dirname, 'data');
const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
const SERVERS_FILE = path.join(DATA_DIR, 'servers.json');
const FRIENDS_FILE = path.join(DATA_DIR, 'friends.json');
const DM_FILE = path.join(DATA_DIR, 'dm.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function loadJSON(file, def = {}) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) { console.error('Load error:', file, e.message); }
  return def;
}

function saveJSON(file, data) {
  try {
    fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) { console.error('Save error:', file, e.message); }
}

// ============ STATE ============
const accounts = new Map(useDB ? [] : Object.entries(loadJSON(ACCOUNTS_FILE)));
const servers = new Map();
const friends = new Map();
const friendRequests = new Map();
const dmHistory = new Map();
const onlineUsers = new Map();
const voiceState = new Map();
const invites = new Map();
const blockedUsers = new Map();

// Load from JSON if no DB
if (!useDB) {
  Object.entries(loadJSON(SERVERS_FILE)).forEach(([id, srv]) => {
    // Migrate messages without time field
    if (srv.messages) {
      Object.keys(srv.messages).forEach(channelId => {
        srv.messages[channelId] = (srv.messages[channelId] || []).map(msg => {
          if (!msg.time && !msg.timestamp) {
            msg.time = Date.now() - Math.random() * 86400000; // Random time in last 24h
          }
          return msg;
        });
      });
    }
    
    servers.set(id, {
      ...srv,
      members: new Set(srv.members || []),
      roles: srv.roles || [
        { id: 'owner', name: 'Владелец', color: '#f1c40f', position: 100, permissions: ['all'] },
        { id: 'admin', name: 'Админ', color: '#e74c3c', position: 50, permissions: ['manage_channels', 'kick', 'ban', 'manage_messages'] },
        { id: 'moderator', name: 'Модератор', color: '#3498db', position: 25, permissions: ['manage_messages', 'kick'] },
        { id: 'default', name: 'Участник', color: '#99aab5', position: 0, permissions: ['send_messages', 'read_messages'] }
      ],
      memberRoles: srv.memberRoles || {},
      bans: new Set(srv.bans || [])
    });
  });

  const friendsData = loadJSON(FRIENDS_FILE, { friends: {}, requests: {}, blocked: {} });
  Object.entries(friendsData.friends || {}).forEach(([id, arr]) => {
    friends.set(id, new Set(arr));
  });
  Object.entries(friendsData.requests || {}).forEach(([id, arr]) => {
    friendRequests.set(id, new Set(arr));
  });
  Object.entries(friendsData.blocked || {}).forEach(([id, arr]) => {
    blockedUsers.set(id, new Set(arr));
  });

  Object.entries(loadJSON(DM_FILE)).forEach(([key, msgs]) => {
    dmHistory.set(key, msgs);
  });
}

// ============ SAVE ============
function saveAll() {
  console.log('saveAll called, useDB:', useDB);
  if (useDB) {
    saveToDB().catch(function(e) {
      console.error('saveToDB error:', e);
    });
    return;
  }
  
  const accObj = {};
  accounts.forEach((v, k) => { accObj[k] = v; });
  saveJSON(ACCOUNTS_FILE, accObj);
  
  const srvObj = {};
  servers.forEach((srv, id) => {
    srvObj[id] = { ...srv, members: [...srv.members], bans: [...(srv.bans || [])] };
  });
  saveJSON(SERVERS_FILE, srvObj);
  
  const frObj = {};
  friends.forEach((set, id) => { frObj[id] = [...set]; });
  const reqObj = {};
  friendRequests.forEach((set, id) => { reqObj[id] = [...set]; });
  const blkObj = {};
  blockedUsers.forEach((set, id) => { blkObj[id] = [...set]; });
  saveJSON(FRIENDS_FILE, { friends: frObj, requests: reqObj, blocked: blkObj });
  
  const dmObj = {};
  dmHistory.forEach((msgs, key) => { dmObj[key] = msgs; });
  saveJSON(DM_FILE, dmObj);
}

setInterval(function() {
  console.log('Auto-save triggered');
  saveAll();
}, 30000);
process.on('SIGINT', () => { saveAll(); process.exit(); });
process.on('SIGTERM', () => { saveAll(); process.exit(); });

// ============ UTILS ============
function hash(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

function genId(prefix = 'id') {
  return prefix + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
}

function genInvite() {
  return crypto.randomBytes(4).toString('hex');
}

function getDMKey(id1, id2) {
  return [id1, id2].sort().join(':');
}

function getAccountById(userId) {
  // First check accounts
  for (const acc of accounts.values()) {
    if (acc.id === userId) return acc;
  }
  // Fallback to online users
  if (onlineUsers.has(userId)) {
    const online = onlineUsers.get(userId);
    return { id: userId, name: online.name, avatar: online.avatar, status: online.status };
  }
  return null;
}

function getAccountByName(name) {
  for (const acc of accounts.values()) {
    if (acc.name.toLowerCase() === name.toLowerCase()) return acc;
  }
  return null;
}

function escapeHtml(text) {
  if (!text) return '';
  return text.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Profanity filter (basic)
const badWords = ['spam', 'badword'];
function filterMessage(text) {
  let filtered = text;
  badWords.forEach(word => {
    const regex = new RegExp(word, 'gi');
    filtered = filtered.replace(regex, '*'.repeat(word.length));
  });
  return filtered;
}


// ============ PERMISSIONS ============
function hasPermission(serverId, userId, permission) {
  const srv = servers.get(serverId);
  if (!srv) return false;
  if (srv.ownerId === userId) return true;
  
  const roleId = srv.memberRoles[userId] || 'default';
  const role = srv.roles.find(r => r.id === roleId);
  if (!role) return false;
  
  return role.permissions.includes('all') || role.permissions.includes(permission);
}

function canManageChannel(serverId, userId) {
  return hasPermission(serverId, userId, 'manage_channels');
}

function canKick(serverId, userId) {
  return hasPermission(serverId, userId, 'kick');
}

function canBan(serverId, userId) {
  return hasPermission(serverId, userId, 'ban');
}

function canManageMessages(serverId, userId) {
  return hasPermission(serverId, userId, 'manage_messages');
}

function canManageRoles(serverId, userId) {
  return hasPermission(serverId, userId, 'manage_roles');
}

// ============ HTTP SERVER ============
const MIME = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
  '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon'
};

const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }
  
  // API endpoints
  if (req.url.startsWith('/api/')) {
    handleAPI(req, res);
    return;
  }
  
  // Remove query parameters from URL
  let urlPath = req.url.split('?')[0];
  let filePath = urlPath === '/' ? '/index.html' : urlPath;
  filePath = path.join(__dirname, 'public', filePath);
  const ext = path.extname(filePath);
  
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ============ API HANDLERS ============
function handleAPI(req, res) {
  res.setHeader('Content-Type', 'application/json');
  
  if (req.url === '/api/status') {
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'ok', users: onlineUsers.size, servers: servers.size }));
    return;
  }
  
  if (req.url === '/api/servers') {
    const list = [];
    servers.forEach((srv, id) => {
      list.push({ id, name: srv.name, members: srv.members.size });
    });
    res.writeHead(200);
    res.end(JSON.stringify(list));
    return;
  }
  
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
}


// ============ WEBSOCKET ============
const wss = new WebSocket.Server({ server: httpServer });

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function sendToUser(userId, data) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.userId === userId) {
      client.send(JSON.stringify(data));
    }
  });
}

function broadcastToServer(serverId, data, excludeId = null) {
  const srv = servers.get(serverId);
  if (!srv) return;
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && srv.members.has(client.userId) && client.userId !== excludeId) {
      client.send(msg);
    }
  });
}

function broadcast(data, excludeId = null) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.userId !== excludeId) {
      client.send(msg);
    }
  });
}

// ============ HELPERS ============
function getUserData(userId) {
  const acc = getAccountById(userId);
  const isOnline = onlineUsers.has(userId);
  return acc ? {
    id: userId,
    name: acc.name,
    avatar: acc.avatar,
    status: isOnline ? (acc.status || 'online') : 'offline',
    customStatus: acc.customStatus || null
  } : null;
}

function getFriendsList(userId) {
  const myFriends = friends.get(userId) || new Set();
  return [...myFriends].map(fid => getUserData(fid)).filter(Boolean);
}

function getPendingRequests(userId) {
  const reqs = friendRequests.get(userId) || new Set();
  return [...reqs].map(fid => getUserData(fid)).filter(Boolean);
}

function getServersForUser(userId) {
  const result = {};
  servers.forEach((srv, id) => {
    if (srv.members.has(userId)) {
      result[id] = {
        id: srv.id,
        name: srv.name,
        icon: srv.icon,
        ownerId: srv.ownerId,
        channels: srv.channels || [],
        voiceChannels: srv.voiceChannels || [],
        messages: srv.messages || {},
        members: [...srv.members],
        roles: srv.roles || [],
        memberRoles: srv.memberRoles || {},
        channelPermissions: srv.channelPermissions || {}
      };
    }
  });
  return result;
}

function getServerMembers(serverId, requesterId) {
  const srv = servers.get(serverId);
  if (!srv) return [];
  return [...srv.members].map(id => {
    const acc = getAccountById(id);
    const isOnline = onlineUsers.has(id) || id === requesterId;
    return acc ? {
      id,
      name: acc.name,
      avatar: acc.avatar,
      status: isOnline ? (acc.status || 'online') : 'offline',
      customStatus: acc.customStatus,
      role: srv.memberRoles[id] || 'default',
      isOwner: srv.ownerId === id
    } : null;
  }).filter(Boolean);
}

function getVoiceUsers(serverId, channelId) {
  const result = [];
  voiceState.forEach((data, oderId) => {
    if (data.serverId === serverId && data.channelId === channelId) {
      const user = getUserData(oderId);
      if (user) result.push({ id: oderId, oderId: oderId, ...user, muted: data.muted, video: data.video, screen: data.screen });
    }
  });
  return result;
}


// ============ MESSAGE HANDLERS ============
const handlers = {
  ping(ws) {
    send(ws, { type: 'pong' });
  },

  register(ws, data) {
    const { email, password, name } = data;
    if (accounts.has(email)) {
      send(ws, { type: 'auth_error', message: 'Email уже зарегистрирован' });
      return;
    }
    
    const userId = genId('user');
    const account = {
      id: userId,
      email,
      password: hash(password),
      name: name || 'Пользователь',
      avatar: null,
      status: 'online',
      customStatus: null,
      createdAt: Date.now(),
      settings: { notifications: true, sounds: true, privacy: 'everyone' }
    };
    accounts.set(email, account);
    saveAll();
    
    ws.userId = userId;
    onlineUsers.set(userId, { name: account.name, avatar: account.avatar, status: 'online' });
    
    send(ws, {
      type: 'auth_success',
      userId,
      user: { name: account.name, avatar: account.avatar, status: 'online' },
      servers: getServersForUser(userId),
      friends: getFriendsList(userId),
      pendingRequests: getPendingRequests(userId)
    });
    
    broadcast({ type: 'user_join', user: getUserData(userId) }, userId);
  },

  login(ws, data) {
    const { email, password } = data;
    const account = accounts.get(email);
    
    if (!account || account.password !== hash(password)) {
      send(ws, { type: 'auth_error', message: 'Неверный email или пароль' });
      return;
    }
    
    const userId = account.id;
    ws.userId = userId;
    onlineUsers.set(userId, { name: account.name, avatar: account.avatar, status: account.status || 'online' });
    
    send(ws, {
      type: 'auth_success',
      userId,
      user: { name: account.name, avatar: account.avatar, status: account.status || 'online', customStatus: account.customStatus },
      servers: getServersForUser(userId),
      friends: getFriendsList(userId),
      pendingRequests: getPendingRequests(userId)
    });
    
    broadcast({ type: 'user_join', user: getUserData(userId) }, userId);
  },

  guest_login(ws) {
    const guestNum = Math.floor(Math.random() * 10000);
    const userId = genId('guest');
    const name = 'Гость' + guestNum;
    
    ws.userId = userId;
    ws.isGuest = true;
    onlineUsers.set(userId, { name, avatar: null, status: 'online' });
    
    send(ws, {
      type: 'auth_success',
      userId,
      user: { name, avatar: null, status: 'online' },
      servers: {},
      friends: [],
      pendingRequests: [],
      isGuest: true
    });
  },

  message(ws, data) {
    const { serverId, channel, text, replyTo, attachments } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !srv.members.has(userId)) return;
    
    // Check channel permissions
    const chPerms = srv.channelPermissions?.[channel];
    if (chPerms && !chPerms.send.includes(srv.memberRoles[userId] || 'default') && srv.ownerId !== userId) {
      send(ws, { type: 'error', message: 'Нет прав для отправки сообщений' });
      return;
    }
    
    const user = onlineUsers.get(userId);
    const acc = getAccountById(userId);
    const filteredText = filterMessage(text);
    
    const msg = {
      id: Date.now().toString() + '_' + Math.random().toString(36).slice(2, 6),
      oderId: userId,
      author: acc?.name || user?.name || 'User',
      avatar: acc?.avatar || user?.avatar,
      text: filteredText,
      replyTo: replyTo || null,
      attachments: attachments || [],
      reactions: {},
      time: Date.now(),
      edited: false
    };
    
    if (!srv.messages) srv.messages = {};
    if (!srv.messages[channel]) srv.messages[channel] = [];
    srv.messages[channel].push(msg);
    if (srv.messages[channel].length > 500) srv.messages[channel].shift();
    saveAll();
    
    broadcastToServer(serverId, { type: 'message', serverId, channel, message: msg });
  },


  edit_message(ws, data) {
    const { serverId, channelId, messageId, text } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv) return;
    
    const msgs = srv.messages?.[channelId];
    if (!msgs) return;
    
    const msg = msgs.find(m => m.id == messageId);
    if (!msg || msg.oderId !== userId) return;
    
    msg.text = filterMessage(text);
    msg.edited = true;
    msg.editedAt = Date.now();
    saveAll();
    
    broadcastToServer(serverId, { type: 'message_edited', serverId, channelId, messageId, text: msg.text, editedAt: msg.editedAt });
  },

  delete_message(ws, data) {
    const { serverId, channelId, messageId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv) return;
    
    const msgs = srv.messages?.[channelId];
    if (!msgs) return;
    
    const idx = msgs.findIndex(m => m.id == messageId);
    if (idx === -1) return;
    
    const msg = msgs[idx];
    if (msg.oderId !== userId && srv.ownerId !== userId && !canManageMessages(serverId, userId)) return;
    
    msgs.splice(idx, 1);
    msgs.forEach(m => {
      if (m.replyTo && m.replyTo.id == messageId) {
        m.replyTo.deleted = true;
      }
    });
    saveAll();
    
    broadcastToServer(serverId, { type: 'message_deleted', serverId, channelId, messageId });
  },

  add_reaction(ws, data) {
    const { serverId, channelId, messageId, emoji } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv) return;
    
    const msgs = srv.messages?.[channelId];
    if (!msgs) return;
    
    const msg = msgs.find(m => m.id == messageId);
    if (!msg) return;
    
    if (!msg.reactions) msg.reactions = {};
    if (!msg.reactions[emoji]) msg.reactions[emoji] = [];
    
    if (!msg.reactions[emoji].includes(userId)) {
      msg.reactions[emoji].push(userId);
      saveAll();
      broadcastToServer(serverId, { type: 'reaction_added', serverId, channelId, messageId, emoji, userId });
    }
  },

  remove_reaction(ws, data) {
    const { serverId, channelId, messageId, emoji } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv) return;
    
    const msgs = srv.messages?.[channelId];
    if (!msgs) return;
    
    const msg = msgs.find(m => m.id == messageId);
    if (!msg || !msg.reactions?.[emoji]) return;
    
    const idx = msg.reactions[emoji].indexOf(userId);
    if (idx !== -1) {
      msg.reactions[emoji].splice(idx, 1);
      if (msg.reactions[emoji].length === 0) delete msg.reactions[emoji];
      saveAll();
      broadcastToServer(serverId, { type: 'reaction_removed', serverId, channelId, messageId, emoji, userId });
    }
  },

  dm(ws, data) {
    const { to, text, attachments } = data;
    const userId = ws.userId;
    
    // Check if blocked
    const blocked = blockedUsers.get(to);
    if (blocked && blocked.has(userId)) {
      send(ws, { type: 'dm_error', message: 'Пользователь заблокировал вас' });
      return;
    }
    
    const user = getUserData(userId);
    const recipient = getUserData(to);
    
    const msg = {
      id: Date.now().toString() + '_' + Math.random().toString(36).slice(2, 6),
      from: userId,
      to,
      author: user?.name || 'User',
      avatar: user?.avatar,
      text: filterMessage(text),
      attachments: attachments || [],
      time: Date.now()
    };
    
    const key = getDMKey(userId, to);
    if (!dmHistory.has(key)) dmHistory.set(key, []);
    dmHistory.get(key).push(msg);
    if (dmHistory.get(key).length > 500) dmHistory.get(key).shift();
    saveAll();
    
    sendToUser(to, { type: 'dm', message: msg, sender: user });
    send(ws, { type: 'dm_sent', to, message: msg, recipient });
  },

  get_dm_history(ws, data) {
    const { oderId } = data;
    const userId = ws.userId;
    const key = getDMKey(userId, oderId);
    const msgs = dmHistory.get(key) || [];
    send(ws, { type: 'dm_history', oderId, messages: msgs });
  },


  create_server(ws, data) {
    const userId = ws.userId;
    console.log('Creating server for user:', userId);
    const serverId = genId('server');
    const srv = {
      id: serverId,
      name: data.name || 'Новый сервер',
      icon: data.icon || null,
      region: data.region || 'auto',
      ownerId: userId,
      channels: [{ id: 'general', name: 'общий' }],
      voiceChannels: [{ id: 'voice', name: 'Голосовой' }],
      messages: { general: [] },
      members: new Set([userId]),
      roles: [
        { id: 'owner', name: 'Владелец', color: '#f1c40f', position: 100, permissions: ['all'] },
        { id: 'admin', name: 'Админ', color: '#e74c3c', position: 50, permissions: ['manage_channels', 'kick', 'ban', 'manage_messages', 'manage_roles'] },
        { id: 'moderator', name: 'Модератор', color: '#3498db', position: 25, permissions: ['manage_messages', 'kick'] },
        { id: 'default', name: 'Участник', color: '#99aab5', position: 0, permissions: ['send_messages', 'read_messages'] }
      ],
      memberRoles: { [userId]: 'owner' },
      channelPermissions: {},
      bans: new Set()
    };
    servers.set(serverId, srv);
    saveAll();
    
    send(ws, {
      type: 'server_created',
      server: { ...srv, members: [...srv.members], bans: [] }
    });
  },

  update_server(ws, data) {
    const { serverId, name, icon, region } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || srv.ownerId !== userId) return;
    
    if (name) srv.name = name;
    if (icon !== undefined) srv.icon = icon;
    if (region) srv.region = region;
    saveAll();
    
    broadcastToServer(serverId, { type: 'server_updated', serverId, name: srv.name, icon: srv.icon, region: srv.region });
  },

  delete_server(ws, data) {
    const { serverId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || srv.ownerId !== userId) return;
    
    srv.members.forEach(memberId => {
      sendToUser(memberId, { type: 'server_deleted', serverId });
    });
    servers.delete(serverId);
    saveAll();
  },

  leave_server(ws, data) {
    const { serverId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || srv.ownerId === userId) return;
    
    srv.members.delete(userId);
    delete srv.memberRoles[userId];
    saveAll();
    
    send(ws, { type: 'server_left', serverId });
    broadcastToServer(serverId, { type: 'member_left', serverId, oderId: userId });
  },

  kick_member(ws, data) {
    const { serverId, memberId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canKick(serverId, userId) || memberId === srv.ownerId) return;
    
    srv.members.delete(memberId);
    delete srv.memberRoles[memberId];
    saveAll();
    
    sendToUser(memberId, { type: 'server_left', serverId, kicked: true });
    broadcastToServer(serverId, { type: 'member_left', serverId, oderId: memberId, kicked: true });
  },

  ban_member(ws, data) {
    const { serverId, memberId, reason } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canBan(serverId, userId) || memberId === srv.ownerId) return;
    
    srv.members.delete(memberId);
    delete srv.memberRoles[memberId];
    if (!srv.bans) srv.bans = new Set();
    srv.bans.add(memberId);
    saveAll();
    
    sendToUser(memberId, { type: 'server_left', serverId, banned: true, reason });
    broadcastToServer(serverId, { type: 'member_banned', serverId, oderId: memberId });
  },

  unban_member(ws, data) {
    const { serverId, memberId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canBan(serverId, userId)) return;
    
    if (srv.bans) srv.bans.delete(memberId);
    saveAll();
    
    send(ws, { type: 'member_unbanned', serverId, memberId });
  },


  create_channel(ws, data) {
    const { serverId, name, isVoice, isTemporary } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageChannel(serverId, userId)) return;
    
    const channelId = genId('ch');
    const channel = { id: channelId, name: name || 'новый-канал', isTemporary: isTemporary || false };
    
    if (isVoice) {
      srv.voiceChannels.push(channel);
    } else {
      srv.channels.push(channel);
      srv.messages[channelId] = [];
    }
    saveAll();
    
    broadcastToServer(serverId, { type: 'channel_created', serverId, channel, isVoice });
  },

  update_channel(ws, data) {
    const { serverId, channelId, name, isVoice } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageChannel(serverId, userId)) return;
    
    const channels = isVoice ? srv.voiceChannels : srv.channels;
    const ch = channels.find(c => c.id === channelId);
    if (ch && name) {
      ch.name = name;
      saveAll();
      broadcastToServer(serverId, { type: 'channel_updated', serverId, channelId, name, isVoice });
    }
  },

  delete_channel(ws, data) {
    const { serverId, channelId, isVoice } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageChannel(serverId, userId)) return;
    
    if (isVoice) {
      srv.voiceChannels = srv.voiceChannels.filter(c => c.id !== channelId);
    } else {
      srv.channels = srv.channels.filter(c => c.id !== channelId);
      delete srv.messages[channelId];
    }
    saveAll();
    
    broadcastToServer(serverId, { type: 'channel_deleted', serverId, channelId, isVoice });
  },

  set_channel_permissions(ws, data) {
    const { serverId, channelId, permissions } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageChannel(serverId, userId)) return;
    
    if (!srv.channelPermissions) srv.channelPermissions = {};
    srv.channelPermissions[channelId] = permissions;
    saveAll();
    
    broadcastToServer(serverId, { type: 'channel_permissions_updated', serverId, channelId, permissions });
  },

  create_invite(ws, data) {
    const { serverId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !srv.members.has(userId)) return;
    
    const code = genInvite();
    invites.set(code, { serverId, createdBy: userId, createdAt: Date.now() });
    send(ws, { type: 'invite_created', code, serverId });
  },

  use_invite(ws, data) {
    const { code } = data;
    const userId = ws.userId;
    const invite = invites.get(code);
    
    if (!invite) {
      send(ws, { type: 'invite_error', message: 'Недействительный код' });
      return;
    }
    
    const srv = servers.get(invite.serverId);
    if (!srv) return;
    
    if (srv.bans && srv.bans.has(userId)) {
      send(ws, { type: 'invite_error', message: 'Вы забанены на этом сервере' });
      return;
    }
    
    srv.members.add(userId);
    srv.memberRoles[userId] = 'default';
    saveAll();
    
    send(ws, {
      type: 'server_joined',
      serverId: invite.serverId,
      server: { ...srv, members: [...srv.members], bans: [] }
    });
    
    broadcastToServer(invite.serverId, {
      type: 'member_joined',
      serverId: invite.serverId,
      user: getUserData(userId)
    }, userId);
  },

  // Roles management
  create_role(ws, data) {
    const { serverId, name, color, permissions } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageRoles(serverId, userId)) return;
    
    const roleId = genId('role');
    const maxPos = Math.max(...srv.roles.map(r => r.position));
    const role = {
      id: roleId,
      name: name || 'Новая роль',
      color: color || '#99aab5',
      position: maxPos - 1,
      permissions: permissions || ['send_messages', 'read_messages']
    };
    srv.roles.push(role);
    saveAll();
    
    broadcastToServer(serverId, { type: 'role_created', serverId, role });
  },

  update_role(ws, data) {
    const { serverId, roleId, name, color, permissions } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageRoles(serverId, userId)) return;
    
    const role = srv.roles.find(r => r.id === roleId);
    if (!role || role.id === 'owner') return;
    
    if (name) role.name = name;
    if (color) role.color = color;
    if (permissions) role.permissions = permissions;
    saveAll();
    
    broadcastToServer(serverId, { type: 'role_updated', serverId, role });
  },

  delete_role(ws, data) {
    const { serverId, roleId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageRoles(serverId, userId)) return;
    if (roleId === 'owner' || roleId === 'default') return;
    
    srv.roles = srv.roles.filter(r => r.id !== roleId);
    Object.keys(srv.memberRoles).forEach(mid => {
      if (srv.memberRoles[mid] === roleId) srv.memberRoles[mid] = 'default';
    });
    saveAll();
    
    broadcastToServer(serverId, { type: 'role_deleted', serverId, roleId });
  },

  assign_role(ws, data) {
    const { serverId, memberId, roleId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !canManageRoles(serverId, userId)) return;
    if (memberId === srv.ownerId) return;
    
    const role = srv.roles.find(r => r.id === roleId);
    if (!role) return;
    
    srv.memberRoles[memberId] = roleId;
    saveAll();
    
    broadcastToServer(serverId, { type: 'role_assigned', serverId, memberId, roleId });
  },


  // Friends
  friend_request(ws, data) {
    const { name, to } = data;
    const userId = ws.userId;
    
    console.log('friend_request from', userId, 'to:', to, 'name:', name);
    console.log('accounts size:', accounts.size, 'onlineUsers size:', onlineUsers.size);
    
    // Find target by name or by ID
    let target;
    if (to) {
      target = getAccountById(to);
      console.log('Found by ID:', target ? target.name : 'not found');
    } else if (name) {
      target = getAccountByName(name);
      console.log('Found by name:', target ? target.id : 'not found');
    }
    
    if (!target) {
      send(ws, { type: 'friend_error', message: 'Пользователь не найден' });
      return;
    }
    if (target.id === userId) {
      send(ws, { type: 'friend_error', message: 'Нельзя добавить себя' });
      return;
    }
    
    // Check privacy settings
    if (target.settings?.privacy === 'friends_only') {
      send(ws, { type: 'friend_error', message: 'Пользователь не принимает заявки' });
      return;
    }
    
    // Check if blocked
    const blocked = blockedUsers.get(target.id);
    if (blocked && blocked.has(userId)) {
      send(ws, { type: 'friend_error', message: 'Пользователь заблокировал вас' });
      return;
    }
    
    const myFriends = friends.get(userId) || new Set();
    if (myFriends.has(target.id)) {
      send(ws, { type: 'friend_error', message: 'Уже в друзьях' });
      return;
    }
    
    // Check if request already sent
    const existingReqs = friendRequests.get(target.id);
    if (existingReqs && existingReqs.has(userId)) {
      send(ws, { type: 'friend_error', message: 'Запрос уже отправлен' });
      return;
    }
    
    if (!friendRequests.has(target.id)) friendRequests.set(target.id, new Set());
    friendRequests.get(target.id).add(userId);
    saveAll();
    
    sendToUser(target.id, {
      type: 'friend_request_incoming',
      from: userId,
      user: getUserData(userId)
    });
    send(ws, { type: 'friend_request_sent', to: target.id });
  },

  friend_accept(ws, data) {
    const { from } = data;
    const userId = ws.userId;
    console.log('friend_accept:', userId, 'accepting', from);
    
    const reqs = friendRequests.get(userId);
    console.log('pending requests for', userId, ':', reqs ? [...reqs] : 'none');
    
    if (!reqs || !reqs.has(from)) {
      console.log('Request not found, checking all requests...');
      // Try to find the request
      let found = false;
      if (reqs) {
        for (const reqId of reqs) {
          if (reqId === from || String(reqId) === String(from)) {
            found = true;
            reqs.delete(reqId);
            break;
          }
        }
      }
      if (!found) {
        send(ws, { type: 'friend_error', message: 'Запрос не найден' });
        return;
      }
    } else {
      reqs.delete(from);
    }
    
    if (!friends.has(userId)) friends.set(userId, new Set());
    if (!friends.has(from)) friends.set(from, new Set());
    friends.get(userId).add(from);
    friends.get(from).add(userId);
    saveAll();
    
    console.log('Friend added:', userId, '<->', from);
    send(ws, { type: 'friend_added', user: getUserData(from) });
    sendToUser(from, { type: 'friend_added', user: getUserData(userId) });
  },

  friend_reject(ws, data) {
    const { from } = data;
    const userId = ws.userId;
    const reqs = friendRequests.get(userId);
    if (reqs) {
      reqs.delete(from);
      saveAll();
    }
  },

  friend_remove(ws, data) {
    const { oderId } = data;
    const userId = ws.userId;
    
    const myFriends = friends.get(userId);
    const theirFriends = friends.get(oderId);
    if (myFriends) myFriends.delete(oderId);
    if (theirFriends) theirFriends.delete(userId);
    saveAll();
    
    send(ws, { type: 'friend_removed', oderId });
    sendToUser(oderId, { type: 'friend_removed', oderId: userId });
  },

  block_user(ws, data) {
    const { oderId } = data;
    const userId = ws.userId;
    
    if (!blockedUsers.has(userId)) blockedUsers.set(userId, new Set());
    blockedUsers.get(userId).add(oderId);
    
    // Remove from friends
    const myFriends = friends.get(userId);
    const theirFriends = friends.get(oderId);
    if (myFriends) myFriends.delete(oderId);
    if (theirFriends) theirFriends.delete(userId);
    saveAll();
    
    send(ws, { type: 'user_blocked', oderId });
  },

  unblock_user(ws, data) {
    const { oderId } = data;
    const userId = ws.userId;
    
    const blocked = blockedUsers.get(userId);
    if (blocked) blocked.delete(oderId);
    saveAll();
    
    send(ws, { type: 'user_unblocked', oderId });
  },

  get_friends(ws) {
    const userId = ws.userId;
    send(ws, {
      type: 'friends_list',
      friends: getFriendsList(userId),
      requests: getPendingRequests(userId)
    });
  },

  get_server_members(ws, data) {
    const { serverId } = data;
    const userId = ws.userId;
    send(ws, {
      type: 'server_members',
      serverId,
      members: getServerMembers(serverId, userId)
    });
  },

  update_profile(ws, data) {
    const { name, avatar, status, customStatus } = data;
    const userId = ws.userId;
    const acc = getAccountById(userId);
    if (!acc) return;
    
    if (name) acc.name = name;
    if (avatar !== undefined) acc.avatar = avatar;
    if (status) acc.status = status;
    if (customStatus !== undefined) acc.customStatus = customStatus;
    
    const online = onlineUsers.get(userId);
    if (online) {
      if (name) online.name = name;
      if (avatar !== undefined) online.avatar = avatar;
      if (status) online.status = status;
    }
    
    servers.forEach(srv => {
      if (srv.members.has(userId)) {
        Object.values(srv.messages || {}).forEach(msgs => {
          msgs.forEach(msg => {
            if (msg.oderId === userId) {
              msg.author = acc.name;
              msg.avatar = acc.avatar;
            }
          });
        });
      }
    });
    saveAll();
    
    send(ws, { type: 'profile_updated', user: getUserData(userId) });
    broadcast({ type: 'user_update', user: getUserData(userId) }, userId);
  },

  update_settings(ws, data) {
    const { settings } = data;
    const userId = ws.userId;
    const acc = getAccountById(userId);
    if (!acc) return;
    
    acc.settings = { ...acc.settings, ...settings };
    saveAll();
    
    send(ws, { type: 'settings_updated', settings: acc.settings });
  },


  // Voice & Video
  voice_join(ws, data) {
    const { serverId, channelId } = data;
    const userId = ws.userId;
    voiceState.set(userId, { serverId, channelId, muted: false, video: false, screen: false });
    
    // Check if temporary channel and create if needed
    const srv = servers.get(serverId);
    if (srv) {
      const ch = srv.voiceChannels.find(c => c.id === channelId);
      if (ch?.isTemporary) {
        // Temporary channel logic
      }
    }
    
    broadcastToServer(serverId, {
      type: 'voice_state_update',
      serverId,
      channelId,
      users: getVoiceUsers(serverId, channelId)
    });
  },

  voice_leave(ws) {
    const userId = ws.userId;
    const state = voiceState.get(userId);
    if (state) {
      voiceState.delete(userId);
      
      // Check if temporary channel should be deleted
      const srv = servers.get(state.serverId);
      if (srv) {
        const ch = srv.voiceChannels.find(c => c.id === state.channelId);
        if (ch?.isTemporary) {
          const usersInChannel = getVoiceUsers(state.serverId, state.channelId);
          if (usersInChannel.length === 0) {
            srv.voiceChannels = srv.voiceChannels.filter(c => c.id !== state.channelId);
            broadcastToServer(state.serverId, { type: 'channel_deleted', serverId: state.serverId, channelId: state.channelId, isVoice: true });
          }
        }
      }
      
      broadcastToServer(state.serverId, {
        type: 'voice_state_update',
        serverId: state.serverId,
        channelId: state.channelId,
        users: getVoiceUsers(state.serverId, state.channelId)
      });
    }
  },

  voice_mute(ws, data) {
    const userId = ws.userId;
    const state = voiceState.get(userId);
    if (state) {
      state.muted = data.muted;
      broadcastToServer(state.serverId, {
        type: 'voice_state_update',
        serverId: state.serverId,
        channelId: state.channelId,
        users: getVoiceUsers(state.serverId, state.channelId)
      });
    }
  },

  voice_video(ws, data) {
    const userId = ws.userId;
    const state = voiceState.get(userId);
    if (state) {
      state.video = data.video;
      broadcastToServer(state.serverId, {
        type: 'voice_state_update',
        serverId: state.serverId,
        channelId: state.channelId,
        users: getVoiceUsers(state.serverId, state.channelId)
      });
    }
  },

  voice_screen(ws, data) {
    const userId = ws.userId;
    const state = voiceState.get(userId);
    if (state) {
      state.screen = data.screen;
      broadcastToServer(state.serverId, {
        type: 'voice_screen_update',
        serverId: state.serverId,
        channelId: state.channelId,
        userId,
        screen: data.screen
      });
    }
  },

  voice_signal(ws, data) {
    sendToUser(data.to, { type: 'voice_signal', from: ws.userId, signal: data.signal });
  },

  // Search
  search_messages(ws, data) {
    const { serverId, query, channelId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !srv.members.has(userId)) return;
    
    const results = [];
    const searchIn = channelId ? { [channelId]: srv.messages[channelId] } : srv.messages;
    
    Object.entries(searchIn).forEach(([chId, msgs]) => {
      if (!msgs) return;
      msgs.forEach(msg => {
        if (msg.text && msg.text.toLowerCase().includes(query.toLowerCase())) {
          results.push({ ...msg, channelId: chId });
        }
      });
    });
    
    send(ws, { type: 'search_results', serverId, results: results.slice(-50) });
  },

  search_users(ws, data) {
    const { query } = data;
    const results = [];
    
    accounts.forEach(acc => {
      if (acc.name.toLowerCase().includes(query.toLowerCase())) {
        results.push({ id: acc.id, name: acc.name, avatar: acc.avatar });
      }
    });
    
    send(ws, { type: 'user_search_results', results: results.slice(0, 20) });
  },

  // Forward message
  forward_message(ws, data) {
    const { messageId, targetType, targetId, originalServerId, originalChannelId } = data;
    const userId = ws.userId;
    
    // Get original message
    let originalMsg = null;
    if (originalServerId) {
      const srv = servers.get(originalServerId);
      if (srv && srv.messages[originalChannelId]) {
        originalMsg = srv.messages[originalChannelId].find(m => m.id == messageId);
      }
    }
    
    if (!originalMsg) return;
    
    const forwardedMsg = {
      id: Date.now().toString() + '_' + Math.random().toString(36).slice(2, 6),
      oderId: userId,
      author: onlineUsers.get(userId)?.name || 'User',
      avatar: onlineUsers.get(userId)?.avatar,
      text: originalMsg.text,
      forwarded: { from: originalMsg.author, originalId: messageId },
      time: Date.now()
    };
    
    if (targetType === 'channel') {
      const [srvId, chId] = targetId.split(':');
      const srv = servers.get(srvId);
      if (srv && srv.members.has(userId)) {
        if (!srv.messages[chId]) srv.messages[chId] = [];
        srv.messages[chId].push(forwardedMsg);
        saveAll();
        broadcastToServer(srvId, { type: 'message', serverId: srvId, channel: chId, message: forwardedMsg });
      }
    } else if (targetType === 'dm') {
      const key = getDMKey(userId, targetId);
      if (!dmHistory.has(key)) dmHistory.set(key, []);
      const dmMsg = { ...forwardedMsg, from: userId, to: targetId };
      dmHistory.get(key).push(dmMsg);
      saveAll();
      sendToUser(targetId, { type: 'dm', message: dmMsg, sender: getUserData(userId) });
      send(ws, { type: 'dm_sent', to: targetId, message: dmMsg });
    }
  }
};


// ============ CONNECTION ============
wss.on('connection', (ws) => {
  ws.userId = null;
  ws.isGuest = false;
  
  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);
      if (data.type !== 'ping') {
        console.log('WS message:', data.type, 'from:', ws.userId);
      }
      const handler = handlers[data.type];
      if (handler) {
        handler(ws, data);
      } else {
        console.log('Unknown handler:', data.type);
      }
    } catch (e) {
      console.error('Message error:', e.message);
    }
  });
  
  ws.on('close', () => {
    if (ws.userId) {
      const state = voiceState.get(ws.userId);
      if (state) {
        voiceState.delete(ws.userId);
        
        // Check temporary channel
        const srv = servers.get(state.serverId);
        if (srv) {
          const ch = srv.voiceChannels.find(c => c.id === state.channelId);
          if (ch?.isTemporary) {
            const usersInChannel = getVoiceUsers(state.serverId, state.channelId);
            if (usersInChannel.length === 0) {
              srv.voiceChannels = srv.voiceChannels.filter(c => c.id !== state.channelId);
              broadcastToServer(state.serverId, { type: 'channel_deleted', serverId: state.serverId, channelId: state.channelId, isVoice: true });
            }
          }
        }
        
        broadcastToServer(state.serverId, {
          type: 'voice_state_update',
          serverId: state.serverId,
          channelId: state.channelId,
          users: getVoiceUsers(state.serverId, state.channelId)
        });
      }
      onlineUsers.delete(ws.userId);
      broadcast({ type: 'user_leave', oderId: ws.userId });
    }
  });
});

// ============ START ============
const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => console.log('Server running on port', PORT));