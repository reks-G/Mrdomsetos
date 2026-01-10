const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
const SERVERS_FILE = path.join(DATA_DIR, 'servers.json');
const FRIENDS_FILE = path.join(DATA_DIR, 'friends.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// Load data from files
function loadData(file, defaultValue = {}) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) { console.error(`Error loading ${file}:`, e); }
  return defaultValue;
}

// Save data to file
function saveData(file, data) {
  try {
    fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) { console.error(`Error saving ${file}:`, e); }
}

// Convert Map to object for saving
function mapToObj(map) {
  const obj = {};
  map.forEach((v, k) => { obj[k] = v; });
  return obj;
}

// Convert object to Map
function objToMap(obj) {
  return new Map(Object.entries(obj || {}));
}

// Load persisted data
const accountsData = loadData(ACCOUNTS_FILE, {});
const serversData = loadData(SERVERS_FILE, {});
const friendsData = loadData(FRIENDS_FILE, { friends: {}, requests: {} });

const accounts = objToMap(accountsData);
const users = new Map(); // Online users only
const servers = new Map();
const invites = new Map();
const voiceChannels = new Map();
const friends = new Map();
const friendRequests = new Map();

// Restore servers with Set for members
Object.entries(serversData).forEach(([id, srv]) => {
  servers.set(id, { ...srv, members: new Set(srv.members || []) });
});

// Restore friends
Object.entries(friendsData.friends || {}).forEach(([id, arr]) => {
  friends.set(id, new Set(arr));
});
Object.entries(friendsData.requests || {}).forEach(([id, arr]) => {
  friendRequests.set(id, new Set(arr));
});

// Save all data periodically and on changes
function saveAll() {
  // Save accounts
  saveData(ACCOUNTS_FILE, mapToObj(accounts));
  
  // Save servers (convert Sets to arrays)
  const serversObj = {};
  servers.forEach((srv, id) => {
    serversObj[id] = { ...srv, members: [...srv.members] };
  });
  saveData(SERVERS_FILE, serversObj);
  
  // Save friends
  const friendsObj = {};
  friends.forEach((set, id) => { friendsObj[id] = [...set]; });
  const requestsObj = {};
  friendRequests.forEach((set, id) => { requestsObj[id] = [...set]; });
  saveData(FRIENDS_FILE, { friends: friendsObj, requests: requestsObj });
}

// Auto-save every 30 seconds
setInterval(saveAll, 30000);

// Save on exit
process.on('SIGINT', () => { saveAll(); process.exit(); });
process.on('SIGTERM', () => { saveAll(); process.exit(); });

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('MrDomestos* Server');
});

const wss = new WebSocket.Server({ server });

function hashPassword(password) {
  return crypto.createHash('sha256').update(password).digest('hex');
}

function generateInvite() {
  return crypto.randomBytes(4).toString('hex');
}

function broadcast(data, excludeId = null) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.userId !== excludeId) client.send(msg);
  });
}

function broadcastToServer(serverId, data, excludeId = null) {
  const srv = servers.get(serverId);
  if (!srv) return;
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && srv.members.has(client.userId) && client.userId !== excludeId) client.send(msg);
  });
}

function sendTo(targetId, data) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.userId === targetId) client.send(JSON.stringify(data));
  });
}

function getOnlineUsers() {
  const result = [];
  users.forEach((user, id) => {
    result.push({ id, name: user.name, avatar: user.avatar, status: user.status === 'invisible' ? 'offline' : user.status });
  });
  return result;
}

function getServersForUser(uid) {
  const result = {};
  servers.forEach((srv, id) => {
    if (srv.members.has(uid)) {
      // Process messages to mark deleted replies
      const processedMessages = {};
      Object.keys(srv.messages || {}).forEach(channelId => {
        const channelMsgs = srv.messages[channelId] || [];
        const msgIds = new Set(channelMsgs.map(m => m.id));
        processedMessages[channelId] = channelMsgs.map(m => {
          if (m.replyTo && !msgIds.has(m.replyTo.id)) {
            return { ...m, replyTo: { ...m.replyTo, deleted: true } };
          }
          return m;
        });
      });
      result[id] = { id: srv.id, name: srv.name, icon: srv.icon, ownerId: srv.ownerId, channels: srv.channels, voiceChannels: srv.voiceChannels, messages: processedMessages, members: [...srv.members], roles: srv.roles, memberRoles: srv.memberRoles, isMember: true };
    }
  });
  return result;
}

function getVoiceUsers(serverId, channelId) {
  const result = [];
  voiceChannels.forEach((data, oderId) => {
    if (data.serverId === serverId && data.channelId === channelId) {
      const user = users.get(oderId);
      if (user) result.push({ oderId, oderId, name: user.name, avatar: user.avatar, muted: data.muted, speaking: data.speaking || false });
    }
  });
  return result;
}

wss.on('connection', (ws) => {
  let userId = null;

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);

      switch (data.type) {
        case 'register': {
          const { email, password, name } = data;
          if (accounts.has(email)) {
            ws.send(JSON.stringify({ type: 'auth_error', message: 'Email уже зарегистрирован' }));
            return;
          }
          
          userId = 'user_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
          const account = {
            id: userId,
            email,
            password: hashPassword(password),
            name: name || 'Пользователь',
            avatar: null,
            createdAt: Date.now(),
            settings: { theme: 'dark', micDevice: 'default', speakerDevice: 'default' }
          };
          accounts.set(email, account);
          saveAll(); // Save immediately on registration
          
          ws.userId = userId;
          users.set(userId, { name: account.name, avatar: account.avatar, status: 'online', email });
          
          ws.send(JSON.stringify({
            type: 'auth_success',
            userId,
            user: { name: account.name, avatar: account.avatar, status: 'online' },
            settings: account.settings,
            servers: getServersForUser(userId),
            users: getOnlineUsers()
          }));
          
          broadcast({ type: 'user_join', user: { id: userId, name: account.name, avatar: account.avatar, status: 'online' } }, userId);
          break;
        }

        case 'login': {
          const { email, password } = data;
          const account = accounts.get(email);
          
          if (!account || account.password !== hashPassword(password)) {
            ws.send(JSON.stringify({ type: 'auth_error', message: 'Неверный email или пароль' }));
            return;
          }
          
          userId = account.id;
          ws.userId = userId;
          users.set(userId, { name: account.name, avatar: account.avatar, status: 'online', email });
          
          // Get user's friends
          const userFriends = friends.get(userId) || new Set();
          const friendsList = [...userFriends].map(fid => {
            const friendAcc = [...accounts.values()].find(a => a.id === fid);
            const isOnline = users.has(fid);
            return friendAcc ? { id: fid, name: friendAcc.name, avatar: friendAcc.avatar, status: isOnline ? 'online' : 'offline' } : null;
          }).filter(Boolean);
          
          const pendingReqs = friendRequests.get(userId) || new Set();
          const pendingList = [...pendingReqs].map(fid => {
            const friendAcc = [...accounts.values()].find(a => a.id === fid);
            return friendAcc ? { id: fid, name: friendAcc.name, avatar: friendAcc.avatar } : null;
          }).filter(Boolean);
          
          ws.send(JSON.stringify({
            type: 'auth_success',
            userId,
            user: { name: account.name, avatar: account.avatar, status: 'online' },
            settings: account.settings,
            servers: getServersForUser(userId),
            users: getOnlineUsers(),
            friends: friendsList,
            pendingRequests: pendingList
          }));
          
          broadcast({ type: 'user_join', user: { id: userId, name: account.name, avatar: account.avatar, status: 'online' } }, userId);
          break;
        }

        case 'message': {
          const srv = servers.get(data.serverId);
          if (!srv || !srv.members.has(userId)) return;
          
          const user = users.get(userId);
          const msg = {
            id: Date.now(),
            oderId: userId,
            author: user?.name || 'Гость',
            avatar: user?.avatar,
            text: data.text,
            file: data.file,
            replyTo: data.replyTo || null,
            time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
          };

          if (!srv.messages[data.channel]) srv.messages[data.channel] = [];
          srv.messages[data.channel].push(msg);
          if (srv.messages[data.channel].length > 100) srv.messages[data.channel].shift();
          saveAll();

          broadcastToServer(data.serverId, { type: 'message', serverId: data.serverId, channel: data.channel, message: msg });
          break;
        }

        case 'delete_message': {
          console.log('Delete message request:', data, 'from userId:', userId);
          const srv = servers.get(data.serverId);
          if (!srv) {
            console.log('Server not found');
            return;
          }
          const msgs = srv.messages[data.channelId];
          if (!msgs) {
            console.log('Channel messages not found');
            return;
          }
          const msgIndex = msgs.findIndex(m => m.id == data.messageId);
          console.log('Message index:', msgIndex);
          if (msgIndex === -1) {
            console.log('Message not found');
            return;
          }
          const msg = msgs[msgIndex];
          console.log('Message author:', msg.oderId, 'userId:', userId, 'ownerId:', srv.ownerId);
          if (msg.oderId !== userId && srv.ownerId !== userId) {
            console.log('Not authorized to delete');
            return;
          }
          msgs.splice(msgIndex, 1);
          // Mark replies to this message as deleted
          msgs.forEach(m => {
            if (m.replyTo && m.replyTo.id == data.messageId) {
              m.replyTo.deleted = true;
            }
          });
          saveAll();
          console.log('Sending message_deleted');
          const deleteMsg = { type: 'message_deleted', serverId: data.serverId, channelId: data.channelId, messageId: data.messageId };
          ws.send(JSON.stringify(deleteMsg));
          broadcastToServer(data.serverId, deleteMsg, userId);
          break;
        }

        case 'dm': {
          const user = users.get(userId);
          const msg = { id: Date.now(), from: userId, to: data.to, author: user?.name, avatar: user?.avatar, text: data.text, time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }) };
          sendTo(data.to, { type: 'dm', message: msg });
          ws.send(JSON.stringify({ type: 'dm_sent', to: data.to, message: msg }));
          break;
        }

        case 'create_server': {
          const serverId = 'server_' + Date.now();
          const newServer = {
            id: serverId,
            name: data.name || 'Новый сервер',
            icon: data.icon,
            ownerId: userId,
            channels: [{ id: 'general', name: 'общий' }],
            voiceChannels: [{ id: 'voice', name: 'Голосовой' }],
            messages: { general: [] },
            members: new Set([userId]),
            roles: [{ id: 'owner', name: 'Владелец', color: '#f1c40f', position: 100 }, { id: 'default', name: 'Участник', color: '#99aab5', position: 0 }],
            memberRoles: { [userId]: 'owner' }
          };
          servers.set(serverId, newServer);
          saveAll();
          
          ws.send(JSON.stringify({ type: 'server_created', server: { ...newServer, members: [...newServer.members], isMember: true } }));
          break;
        }

        case 'join_server': {
          const srv = servers.get(data.serverId);
          if (!srv) return;
          srv.members.add(userId);
          if (!srv.memberRoles[userId]) srv.memberRoles[userId] = 'default';
          saveAll();
          
          ws.send(JSON.stringify({ type: 'server_joined', serverId: data.serverId, server: { ...srv, members: [...srv.members], isMember: true } }));
          broadcastToServer(data.serverId, { type: 'member_joined', serverId: data.serverId, user: { id: userId, ...users.get(userId), role: srv.memberRoles[userId] } }, userId);
          break;
        }

        case 'leave_server': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId === userId) return;
          srv.members.delete(userId);
          delete srv.memberRoles[userId];
          saveAll();
          
          ws.send(JSON.stringify({ type: 'server_left', serverId: data.serverId }));
          broadcastToServer(data.serverId, { type: 'member_left', serverId: data.serverId, oderId: userId });
          break;
        }

        case 'kick_member': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId || data.memberId === srv.ownerId) return;
          srv.members.delete(data.memberId);
          delete srv.memberRoles[data.memberId];
          saveAll();
          
          sendTo(data.memberId, { type: 'server_left', serverId: data.serverId, kicked: true });
          broadcastToServer(data.serverId, { type: 'member_left', serverId: data.serverId, oderId: data.memberId, kicked: true });
          break;
        }

        case 'update_server': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          if (data.name) srv.name = data.name;
          if (data.icon !== undefined) srv.icon = data.icon;
          saveAll();
          
          broadcastToServer(data.serverId, { type: 'server_updated', serverId: data.serverId, name: srv.name, icon: srv.icon });
          break;
        }

        case 'delete_server': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          srv.members.forEach(memberId => { sendTo(memberId, { type: 'server_deleted', serverId: data.serverId }); });
          servers.delete(data.serverId);
          saveAll();
          break;
        }

        case 'create_channel': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          const channelId = 'ch_' + Date.now();
          const channel = { id: channelId, name: data.name || 'новый-канал' };
          if (data.isVoice) srv.voiceChannels.push(channel);
          else { srv.channels.push(channel); srv.messages[channelId] = []; }
          saveAll();
          
          broadcastToServer(data.serverId, { type: 'channel_created', serverId: data.serverId, channel, isVoice: data.isVoice });
          break;
        }

        case 'delete_channel': {
          console.log('Delete channel request:', data, 'from userId:', userId);
          const srv = servers.get(data.serverId);
          console.log('Server found:', !!srv, 'ownerId:', srv?.ownerId, 'userId:', userId);
          if (!srv) {
            console.log('Server not found');
            return;
          }
          if (srv.ownerId !== userId) {
            console.log('Not owner - denied');
            return;
          }
          console.log('Deleting channel...');
          if (data.isVoice) {
            srv.voiceChannels = srv.voiceChannels.filter(ch => ch.id !== data.channelId);
          } else {
            srv.channels = srv.channels.filter(ch => ch.id !== data.channelId);
            delete srv.messages[data.channelId];
          }
          saveAll();
          console.log('Sending channel_deleted to client');
          const deleteMsg = { type: 'channel_deleted', serverId: data.serverId, channelId: data.channelId, isVoice: data.isVoice };
          ws.send(JSON.stringify(deleteMsg));
          broadcastToServer(data.serverId, deleteMsg, userId);
          console.log('Done');
          break;
        }

        case 'update_channel': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          const channels = data.isVoice ? srv.voiceChannels : srv.channels;
          const ch = channels.find(c => c.id === data.channelId);
          if (ch) {
            if (data.name) ch.name = data.name;
            if (data.topic !== undefined) ch.topic = data.topic;
            saveAll();
            broadcastToServer(data.serverId, { type: 'channel_updated', serverId: data.serverId, channelId: data.channelId, name: ch.name, topic: ch.topic, isVoice: data.isVoice });
          }
          break;
        }

        case 'create_invite': {
          const srv = servers.get(data.serverId);
          if (!srv || !srv.members.has(userId)) return;
          const code = generateInvite();
          invites.set(code, data.serverId);
          ws.send(JSON.stringify({ type: 'invite_created', code, serverId: data.serverId }));
          break;
        }

        case 'use_invite': {
          const serverId = invites.get(data.code);
          if (!serverId) { ws.send(JSON.stringify({ type: 'invite_error', message: 'Недействительный код' })); return; }
          const srv = servers.get(serverId);
          if (!srv) return;
          srv.members.add(userId);
          srv.memberRoles[userId] = 'default';
          saveAll();
          
          ws.send(JSON.stringify({ type: 'server_joined', serverId, server: { ...srv, members: [...srv.members], isMember: true } }));
          break;
        }

        case 'voice_join': {
          voiceChannels.set(userId, { serverId: data.serverId, channelId: data.channelId, muted: false });
          broadcastToServer(data.serverId, { type: 'voice_state_update', serverId: data.serverId, channelId: data.channelId, users: getVoiceUsers(data.serverId, data.channelId) });
          break;
        }

        case 'voice_leave': {
          const voiceData = voiceChannels.get(userId);
          if (voiceData) {
            voiceChannels.delete(userId);
            broadcastToServer(voiceData.serverId, { type: 'voice_state_update', serverId: voiceData.serverId, channelId: voiceData.channelId, users: getVoiceUsers(voiceData.serverId, voiceData.channelId) });
          }
          break;
        }

        case 'voice_mute': {
          const voiceData = voiceChannels.get(userId);
          if (voiceData) {
            voiceData.muted = data.muted;
            broadcastToServer(voiceData.serverId, { type: 'voice_state_update', serverId: voiceData.serverId, channelId: voiceData.channelId, users: getVoiceUsers(voiceData.serverId, voiceData.channelId) });
          }
          break;
        }

        case 'voice_signal': {
          sendTo(data.to, { type: 'voice_signal', from: userId, signal: data.signal });
          break;
        }

        case 'update_profile': {
          const user = users.get(userId);
          if (!user) return;
          const oldName = user.name;
          const oldAvatar = user.avatar;
          user.name = data.name || user.name;
          user.avatar = data.avatar !== undefined ? data.avatar : user.avatar;
          user.status = data.status || user.status;
          
          // Update account
          const account = accounts.get(user.email);
          if (account) { account.name = user.name; account.avatar = user.avatar; }
          
          // Update all messages from this user in all servers
          servers.forEach(srv => {
            if (srv.members.has(userId)) {
              Object.keys(srv.messages).forEach(channelId => {
                srv.messages[channelId].forEach(msg => {
                  if (msg.oderId === userId) {
                    msg.author = user.name;
                    msg.avatar = user.avatar;
                  }
                });
              });
            }
          });
          
          saveAll();
          
          // Send confirmation to the user who updated their profile
          ws.send(JSON.stringify({ type: 'profile_updated', user: { id: userId, name: user.name, avatar: user.avatar, status: user.status } }));
          
          // Broadcast to others
          broadcast({ type: 'user_update', user: { id: userId, ...user } }, userId);
          break;
        }

        case 'update_settings': {
          const user = users.get(userId);
          if (!user) return;
          const account = accounts.get(user.email);
          if (account) {
            account.settings = { ...account.settings, ...data.settings };
            saveAll();
            ws.send(JSON.stringify({ type: 'settings_updated', settings: account.settings }));
          }
          break;
        }

        case 'friend_request': {
          let targetId = null;
          accounts.forEach(acc => { if (acc.name.toLowerCase() === data.name.toLowerCase()) targetId = acc.id; });
          
          if (!targetId) { ws.send(JSON.stringify({ type: 'friend_error', message: 'Пользователь не найден' })); return; }
          if (targetId === userId) { ws.send(JSON.stringify({ type: 'friend_error', message: 'Нельзя добавить себя' })); return; }
          
          const myFriends = friends.get(userId) || new Set();
          if (myFriends.has(targetId)) { ws.send(JSON.stringify({ type: 'friend_error', message: 'Уже в друзьях' })); return; }
          
          if (!friendRequests.has(targetId)) friendRequests.set(targetId, new Set());
          friendRequests.get(targetId).add(userId);
          saveAll();
          
          const myAcc = [...accounts.values()].find(a => a.id === userId);
          sendTo(targetId, { type: 'friend_request_incoming', from: userId, user: { id: userId, name: myAcc?.name, avatar: myAcc?.avatar } });
          ws.send(JSON.stringify({ type: 'friend_request_sent', to: targetId }));
          break;
        }

        case 'friend_accept': {
          const fromId = data.from;
          const requests = friendRequests.get(userId);
          if (!requests || !requests.has(fromId)) return;
          
          requests.delete(fromId);
          if (!friends.has(userId)) friends.set(userId, new Set());
          if (!friends.has(fromId)) friends.set(fromId, new Set());
          friends.get(userId).add(fromId);
          friends.get(fromId).add(userId);
          saveAll();
          
          const myAcc = [...accounts.values()].find(a => a.id === userId);
          const theirAcc = [...accounts.values()].find(a => a.id === fromId);
          const isOnline = users.has(fromId);
          
          ws.send(JSON.stringify({ type: 'friend_added', user: { id: fromId, name: theirAcc?.name, avatar: theirAcc?.avatar, status: isOnline ? 'online' : 'offline' } }));
          sendTo(fromId, { type: 'friend_added', user: { id: userId, name: myAcc?.name, avatar: myAcc?.avatar, status: 'online' } });
          break;
        }

        case 'friend_reject': {
          const requests = friendRequests.get(userId);
          if (requests) { requests.delete(data.from); saveAll(); }
          break;
        }

        case 'friend_remove': {
          const myFriends = friends.get(userId);
          const theirFriends = friends.get(data.oderId);
          if (myFriends) myFriends.delete(data.oderId);
          if (theirFriends) theirFriends.delete(userId);
          saveAll();
          
          ws.send(JSON.stringify({ type: 'friend_removed', oderId: data.oderId }));
          sendTo(data.oderId, { type: 'friend_removed', oderId: userId });
          break;
        }

        case 'get_friends': {
          const myFriends = friends.get(userId) || new Set();
          const friendList = [...myFriends].map(fid => {
            const acc = [...accounts.values()].find(a => a.id === fid);
            const isOnline = users.has(fid);
            return acc ? { id: fid, name: acc.name, avatar: acc.avatar, status: isOnline ? 'online' : 'offline' } : null;
          }).filter(Boolean);
          
          const requests = friendRequests.get(userId) || new Set();
          const requestList = [...requests].map(fid => {
            const acc = [...accounts.values()].find(a => a.id === fid);
            return acc ? { id: fid, name: acc.name, avatar: acc.avatar } : null;
          }).filter(Boolean);
          
          ws.send(JSON.stringify({ type: 'friends_list', friends: friendList, requests: requestList }));
          break;
        }

        case 'get_server_members': {
          const srv = servers.get(data.serverId);
          if (!srv) return;
          const memberList = [...srv.members].map(id => {
            const acc = [...accounts.values()].find(a => a.id === id);
            const isOnline = users.has(id);
            return acc ? { id, name: acc.name, avatar: acc.avatar, status: isOnline ? 'online' : 'offline', role: srv.memberRoles[id], isOwner: srv.ownerId === id } : null;
          }).filter(Boolean);
          
          ws.send(JSON.stringify({ type: 'server_members', serverId: data.serverId, members: memberList }));
          break;
        }
      }
    } catch (e) { console.error('Error:', e); }
  });

  ws.on('close', () => {
    if (userId) {
      const voiceData = voiceChannels.get(userId);
      if (voiceData) {
        voiceChannels.delete(userId);
        broadcastToServer(voiceData.serverId, { type: 'voice_state_update', serverId: voiceData.serverId, channelId: voiceData.channelId, users: getVoiceUsers(voiceData.serverId, voiceData.channelId) });
      }
      const user = users.get(userId);
      users.delete(userId);
      if (user?.status !== 'invisible') broadcast({ type: 'user_leave', oderId: userId });
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`MrDomestos* server on port ${PORT}`));
