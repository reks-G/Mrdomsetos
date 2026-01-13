const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ============ DATA STORAGE ============
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
const accounts = new Map(Object.entries(loadJSON(ACCOUNTS_FILE)));
const servers = new Map();
const friends = new Map();
const friendRequests = new Map();
const dmHistory = new Map();
const onlineUsers = new Map();
const voiceState = new Map();
const invites = new Map();

// Load servers with Set for members
Object.entries(loadJSON(SERVERS_FILE)).forEach(([id, srv]) => {
  servers.set(id, { ...srv, members: new Set(srv.members || []) });
});

// Load friends
const friendsData = loadJSON(FRIENDS_FILE, { friends: {}, requests: {} });
Object.entries(friendsData.friends || {}).forEach(([id, arr]) => {
  friends.set(id, new Set(arr));
});
Object.entries(friendsData.requests || {}).forEach(([id, arr]) => {
  friendRequests.set(id, new Set(arr));
});

// Load DM history
Object.entries(loadJSON(DM_FILE)).forEach(([key, msgs]) => {
  dmHistory.set(key, msgs);
});

// ============ SAVE ============
function saveAll() {
  // Accounts
  const accObj = {};
  accounts.forEach((v, k) => { accObj[k] = v; });
  saveJSON(ACCOUNTS_FILE, accObj);
  
  // Servers
  const srvObj = {};
  servers.forEach((srv, id) => {
    srvObj[id] = { ...srv, members: [...srv.members] };
  });
  saveJSON(SERVERS_FILE, srvObj);
  
  // Friends
  const frObj = {};
  friends.forEach((set, id) => { frObj[id] = [...set]; });
  const reqObj = {};
  friendRequests.forEach((set, id) => { reqObj[id] = [...set]; });
  saveJSON(FRIENDS_FILE, { friends: frObj, requests: reqObj });
  
  // DM
  const dmObj = {};
  dmHistory.forEach((msgs, key) => { dmObj[key] = msgs; });
  saveJSON(DM_FILE, dmObj);
}

setInterval(saveAll, 30000);
process.on('SIGINT', () => { saveAll(); process.exit(); });
process.on('SIGTERM', () => { saveAll(); process.exit(); });

// ============ UTILS ============
function hash(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

function genId(prefix = 'id') {
  return prefix + '_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
}

function genInvite() {
  return crypto.randomBytes(4).toString('hex');
}

function getDMKey(id1, id2) {
  return [id1, id2].sort().join(':');
}

function getAccountById(userId) {
  for (const acc of accounts.values()) {
    if (acc.id === userId) return acc;
  }
  return null;
}

function getAccountByName(name) {
  for (const acc of accounts.values()) {
    if (acc.name.toLowerCase() === name.toLowerCase()) return acc;
  }
  return null;
}

// ============ HTTP SERVER ============
const MIME = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
  '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon'
};

const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(__dirname, 'src', filePath);
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
    status: isOnline ? (acc.status || 'online') : 'offline'
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
        memberRoles: srv.memberRoles || {}
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
      status: isOnline ? 'online' : 'offline',
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
      if (user) result.push({ oderId, oderId, ...user, muted: data.muted });
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
      createdAt: Date.now()
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
      user: { name: account.name, avatar: account.avatar, status: account.status || 'online' },
      servers: getServersForUser(userId),
      friends: getFriendsList(userId),
      pendingRequests: getPendingRequests(userId)
    });
    
    broadcast({ type: 'user_join', user: getUserData(userId) }, userId);
  },

  message(ws, data) {
    const { serverId, channel, text, replyTo } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !srv.members.has(userId)) return;
    
    const user = onlineUsers.get(userId);
    const acc = getAccountById(userId);
    const msg = {
      id: Date.now(),
      oderId: userId,
      author: acc?.name || user?.name || 'User',
      avatar: acc?.avatar || user?.avatar,
      text,
      replyTo: replyTo || null,
      time: Date.now()
    };
    
    if (!srv.messages) srv.messages = {};
    if (!srv.messages[channel]) srv.messages[channel] = [];
    srv.messages[channel].push(msg);
    if (srv.messages[channel].length > 100) srv.messages[channel].shift();
    saveAll();
    
    broadcastToServer(serverId, { type: 'message', serverId, channel, message: msg });
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
    if (msg.oderId !== userId && srv.ownerId !== userId) return;
    
    msgs.splice(idx, 1);
    msgs.forEach(m => {
      if (m.replyTo && m.replyTo.id == messageId) {
        m.replyTo.deleted = true;
      }
    });
    saveAll();
    
    broadcastToServer(serverId, { type: 'message_deleted', serverId, channelId, messageId });
  },

  dm(ws, data) {
    const { to, text } = data;
    const userId = ws.userId;
    const user = getUserData(userId);
    const recipient = getUserData(to);
    
    const msg = {
      id: Date.now(),
      from: userId,
      to,
      author: user?.name || 'User',
      avatar: user?.avatar,
      text,
      time: Date.now()
    };
    
    // Save to history
    const key = getDMKey(userId, to);
    if (!dmHistory.has(key)) dmHistory.set(key, []);
    dmHistory.get(key).push(msg);
    if (dmHistory.get(key).length > 200) dmHistory.get(key).shift();
    saveAll();
    
    // Send to recipient
    sendToUser(to, { type: 'dm', message: msg, sender: user });
    
    // Confirm to sender
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
    const serverId = genId('server');
    const srv = {
      id: serverId,
      name: data.name || 'Новый сервер',
      icon: data.icon || null,
      ownerId: userId,
      channels: [{ id: 'general', name: 'общий' }],
      voiceChannels: [{ id: 'voice', name: 'Голосовой' }],
      messages: { general: [] },
      members: new Set([userId]),
      roles: [
        { id: 'owner', name: 'Владелец', color: '#f1c40f', position: 100 },
        { id: 'default', name: 'Участник', color: '#99aab5', position: 0 }
      ],
      memberRoles: { [userId]: 'owner' }
    };
    servers.set(serverId, srv);
    saveAll();
    
    send(ws, {
      type: 'server_created',
      server: { ...srv, members: [...srv.members] }
    });
  },

  update_server(ws, data) {
    const { serverId, name, icon } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || srv.ownerId !== userId) return;
    
    if (name) srv.name = name;
    if (icon !== undefined) srv.icon = icon;
    saveAll();
    
    broadcastToServer(serverId, { type: 'server_updated', serverId, name: srv.name, icon: srv.icon });
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
    if (!srv || srv.ownerId !== userId || memberId === srv.ownerId) return;
    
    srv.members.delete(memberId);
    delete srv.memberRoles[memberId];
    saveAll();
    
    sendToUser(memberId, { type: 'server_left', serverId, kicked: true });
    broadcastToServer(serverId, { type: 'member_left', serverId, oderId: memberId, kicked: true });
  },

  create_channel(ws, data) {
    const { serverId, name, isVoice } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || srv.ownerId !== userId) return;
    
    const channelId = genId('ch');
    const channel = { id: channelId, name: name || 'новый-канал' };
    
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
    if (!srv || srv.ownerId !== userId) return;
    
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
    if (!srv || srv.ownerId !== userId) return;
    
    if (isVoice) {
      srv.voiceChannels = srv.voiceChannels.filter(c => c.id !== channelId);
    } else {
      srv.channels = srv.channels.filter(c => c.id !== channelId);
      delete srv.messages[channelId];
    }
    saveAll();
    
    broadcastToServer(serverId, { type: 'channel_deleted', serverId, channelId, isVoice });
  },

  create_invite(ws, data) {
    const { serverId } = data;
    const userId = ws.userId;
    const srv = servers.get(serverId);
    if (!srv || !srv.members.has(userId)) return;
    
    const code = genInvite();
    invites.set(code, serverId);
    send(ws, { type: 'invite_created', code, serverId });
  },

  use_invite(ws, data) {
    const { code } = data;
    const userId = ws.userId;
    const serverId = invites.get(code);
    
    if (!serverId) {
      send(ws, { type: 'invite_error', message: 'Недействительный код' });
      return;
    }
    
    const srv = servers.get(serverId);
    if (!srv) return;
    
    srv.members.add(userId);
    srv.memberRoles[userId] = 'default';
    saveAll();
    
    send(ws, {
      type: 'server_joined',
      serverId,
      server: { ...srv, members: [...srv.members] }
    });
    
    broadcastToServer(serverId, {
      type: 'member_joined',
      serverId,
      user: getUserData(userId)
    }, userId);
  },

  friend_request(ws, data) {
    const { name } = data;
    const userId = ws.userId;
    const target = getAccountByName(name);
    
    if (!target) {
      send(ws, { type: 'friend_error', message: 'Пользователь не найден' });
      return;
    }
    if (target.id === userId) {
      send(ws, { type: 'friend_error', message: 'Нельзя добавить себя' });
      return;
    }
    
    const myFriends = friends.get(userId) || new Set();
    if (myFriends.has(target.id)) {
      send(ws, { type: 'friend_error', message: 'Уже в друзьях' });
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
    const reqs = friendRequests.get(userId);
    if (!reqs || !reqs.has(from)) return;
    
    reqs.delete(from);
    if (!friends.has(userId)) friends.set(userId, new Set());
    if (!friends.has(from)) friends.set(from, new Set());
    friends.get(userId).add(from);
    friends.get(from).add(userId);
    saveAll();
    
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
    const { name, avatar, status } = data;
    const userId = ws.userId;
    const acc = getAccountById(userId);
    if (!acc) return;
    
    if (name) acc.name = name;
    if (avatar !== undefined) acc.avatar = avatar;
    if (status) acc.status = status;
    
    const online = onlineUsers.get(userId);
    if (online) {
      if (name) online.name = name;
      if (avatar !== undefined) online.avatar = avatar;
      if (status) online.status = status;
    }
    
    // Update messages
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

  voice_join(ws, data) {
    const { serverId, channelId } = data;
    const userId = ws.userId;
    voiceState.set(userId, { serverId, channelId, muted: false });
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

  voice_signal(ws, data) {
    sendToUser(data.to, { type: 'voice_signal', from: ws.userId, signal: data.signal });
  }
};

// ============ CONNECTION ============
wss.on('connection', (ws) => {
  ws.userId = null;
  
  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);
      const handler = handlers[data.type];
      if (handler) handler(ws, data);
    } catch (e) {
      console.error('Message error:', e.message);
    }
  });
  
  ws.on('close', () => {
    if (ws.userId) {
      const state = voiceState.get(ws.userId);
      if (state) {
        voiceState.delete(ws.userId);
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
