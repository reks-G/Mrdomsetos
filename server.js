const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Ruscord Server');
});

const wss = new WebSocket.Server({ server });

const accounts = new Map();
const users = new Map();
const servers = new Map();
const invites = new Map();
const voiceChannels = new Map();
const calls = new Map();
const friends = new Map(); // oderId -> Set of friend IDs
const friendRequests = new Map(); // oderId -> Set of pending request IDs

function hashPassword(password) {
  return crypto.createHash('sha256').update(password).digest('hex');
}

function generateInvite() {
  return crypto.randomBytes(4).toString('hex');
}

function broadcast(data, excludeId = null) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.userId !== excludeId) {
      client.send(msg);
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

function sendTo(targetId, data) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.userId === targetId) {
      client.send(JSON.stringify(data));
    }
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
    if (srv.members.has(uid) || srv.public) {
      result[id] = {
        id: srv.id, name: srv.name, icon: srv.icon, ownerId: srv.ownerId,
        channels: srv.channels, voiceChannels: srv.voiceChannels, messages: srv.messages,
        members: [...srv.members], roles: srv.roles, memberRoles: srv.memberRoles, isMember: srv.members.has(uid)
      };
    }
  });
  return result;
}

function getVoiceUsers(serverId, channelId) {
  const result = [];
  voiceChannels.forEach((data, oderId) => {
    if (data.serverId === serverId && data.channelId === channelId) {
      const user = users.get(oderId);
      if (user) result.push({ oderId, oderId, name: user.name, avatar: user.avatar, muted: data.muted, deafened: data.deafened, speaking: data.speaking || false });
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
          
          userId = 'user_' + Date.now();
          accounts.set(email, {
            password: hashPassword(password),
            id: userId,
            name: name || 'Пользователь',
            avatar: null,
            settings: { theme: 'dark', micDevice: 'default', speakerDevice: 'default' }
          });
          
          ws.userId = userId;
          users.set(userId, { name: name || 'Пользователь', avatar: null, status: 'online', email });
          
          ws.send(JSON.stringify({
            type: 'auth_success',
            userId: userId,
            user: users.get(userId),
            settings: accounts.get(email).settings,
            servers: getServersForUser(userId),
            users: getOnlineUsers()
          }));
          
          broadcast({ type: 'user_join', user: { id: userId, ...users.get(userId) } }, userId);
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
          
          ws.send(JSON.stringify({
            type: 'auth_success',
            userId: userId,
            user: users.get(userId),
            settings: account.settings,
            servers: getServersForUser(userId),
            users: getOnlineUsers()
          }));
          
          broadcast({ type: 'user_join', user: { id: userId, ...users.get(userId) } }, userId);
          break;
        }

        case 'message': {
          const srv = servers.get(data.serverId);
          if (!srv || !srv.members.has(userId)) return;
          
          const user = users.get(userId);
          const msg = {
            id: Date.now(),
            userId: userId,
            author: user?.name || 'Гость',
            avatar: user?.avatar,
            text: data.text,
            file: data.file,
            time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
          };

          if (!srv.messages[data.channel]) srv.messages[data.channel] = [];
          srv.messages[data.channel].push(msg);
          if (srv.messages[data.channel].length > 100) srv.messages[data.channel].shift();

          broadcastToServer(data.serverId, { type: 'message', serverId: data.serverId, channel: data.channel, message: msg });
          break;
        }

        case 'dm': {
          const user = users.get(userId);
          const msg = {
            id: Date.now(),
            from: userId,
            to: data.to,
            author: user?.name,
            avatar: user?.avatar,
            text: data.text,
            file: data.file,
            time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
          };
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
            roles: [
              { id: 'owner', name: 'Владелец', color: '#f1c40f', position: 100 },
              { id: 'default', name: 'Участник', color: '#99aab5', position: 0 }
            ],
            memberRoles: { [userId]: 'owner' },
            public: false
          };
          servers.set(serverId, newServer);
          
          ws.send(JSON.stringify({
            type: 'server_created',
            server: { ...newServer, members: [...newServer.members], isMember: true }
          }));
          break;
        }

        case 'join_server': {
          const srv = servers.get(data.serverId);
          if (!srv) return;
          
          srv.members.add(userId);
          if (!srv.memberRoles[userId]) srv.memberRoles[userId] = 'default';
          
          ws.send(JSON.stringify({
            type: 'server_joined',
            serverId: data.serverId,
            server: { ...srv, members: [...srv.members], isMember: true }
          }));
          
          broadcastToServer(data.serverId, {
            type: 'member_joined',
            serverId: data.serverId,
            user: { id: userId, ...users.get(userId), role: srv.memberRoles[userId] }
          }, userId);
          break;
        }

        case 'leave_server': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId === userId) return;
          
          srv.members.delete(userId);
          delete srv.memberRoles[userId];
          
          ws.send(JSON.stringify({ type: 'server_left', serverId: data.serverId }));
          broadcastToServer(data.serverId, { type: 'member_left', serverId: data.serverId, userId: userId });
          break;
        }

        case 'update_server': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          
          if (data.name) srv.name = data.name;
          if (data.icon !== undefined) srv.icon = data.icon;
          
          broadcastToServer(data.serverId, { type: 'server_updated', serverId: data.serverId, name: srv.name, icon: srv.icon });
          break;
        }


        case 'create_role': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          
          const roleId = 'role_' + Date.now();
          const role = { id: roleId, name: data.name || 'Новая роль', color: data.color || '#99aab5', position: srv.roles.length };
          srv.roles.push(role);
          
          broadcastToServer(data.serverId, { type: 'role_created', serverId: data.serverId, role });
          break;
        }

        case 'update_role': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          
          const role = srv.roles.find(r => r.id === data.roleId);
          if (role && role.id !== 'owner') {
            if (data.name) role.name = data.name;
            if (data.color) role.color = data.color;
            broadcastToServer(data.serverId, { type: 'role_updated', serverId: data.serverId, role });
          }
          break;
        }

        case 'delete_role': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          if (data.roleId === 'owner' || data.roleId === 'default') return;
          
          srv.roles = srv.roles.filter(r => r.id !== data.roleId);
          Object.keys(srv.memberRoles).forEach(uid => {
            if (srv.memberRoles[uid] === data.roleId) srv.memberRoles[uid] = 'default';
          });
          
          broadcastToServer(data.serverId, { type: 'role_deleted', serverId: data.serverId, roleId: data.roleId });
          break;
        }

        case 'assign_role': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          if (data.targetId === srv.ownerId) return;
          
          srv.memberRoles[data.targetId] = data.roleId;
          broadcastToServer(data.serverId, { type: 'role_assigned', serverId: data.serverId, oderId: data.targetId, roleId: data.roleId });
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
          if (!serverId) {
            ws.send(JSON.stringify({ type: 'invite_error', message: 'Недействительный код' }));
            return;
          }
          
          const srv = servers.get(serverId);
          if (!srv) return;
          
          srv.members.add(userId);
          srv.memberRoles[userId] = 'default';
          
          ws.send(JSON.stringify({ type: 'server_joined', serverId, server: { ...srv, members: [...srv.members], isMember: true } }));
          break;
        }

        case 'create_channel': {
          const srv = servers.get(data.serverId);
          if (!srv || srv.ownerId !== userId) return;
          
          const channelId = 'ch_' + Date.now();
          const channel = { id: channelId, name: data.name || 'новый-канал' };
          
          if (data.isVoice) srv.voiceChannels.push(channel);
          else { srv.channels.push(channel); srv.messages[channelId] = []; }
          
          broadcastToServer(data.serverId, { type: 'channel_created', serverId: data.serverId, channel, isVoice: data.isVoice });
          break;
        }

        case 'voice_join': {
          voiceChannels.set(userId, { serverId: data.serverId, channelId: data.channelId, muted: false, deafened: false });
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

        case 'voice_speaking': {
          const voiceData = voiceChannels.get(userId);
          if (voiceData) {
            voiceData.speaking = data.speaking;
            broadcastToServer(voiceData.serverId, { type: 'voice_speaking_update', oderId: oderId, oderId: oderId, speaking: data.speaking, channelId: voiceData.channelId });
          }
          break;
        }

        case 'voice_signal': {
          sendTo(data.to, { type: 'voice_signal', from: userId, signal: data.signal });
          break;
        }

        case 'call_start': {
          const targetId = data.to;
          calls.set(userId, { oderId: targetId, oderId: userId, type: data.callType || 'audio' });
          sendTo(targetId, { type: 'call_incoming', from: userId, callType: data.callType || 'audio', user: users.get(userId) });
          break;
        }

        case 'call_accept': {
          const callerId = data.from;
          sendTo(callerId, { type: 'call_accepted', oderId: userId });
          break;
        }

        case 'call_reject': {
          const callerId = data.from;
          calls.delete(callerId);
          sendTo(callerId, { type: 'call_rejected', oderId: userId });
          break;
        }

        case 'call_end': {
          const call = calls.get(userId) || calls.get(data.to);
          if (call) {
            calls.delete(userId);
            calls.delete(data.to);
            sendTo(data.to, { type: 'call_ended', oderId: userId });
          }
          break;
        }

        case 'call_signal': {
          sendTo(data.to, { type: 'call_signal', from: userId, signal: data.signal });
          break;
        }

        case 'update_profile': {
          const user = users.get(userId);
          if (!user) return;
          
          user.name = data.name || user.name;
          user.avatar = data.avatar !== undefined ? data.avatar : user.avatar;
          user.status = data.status || user.status;
          
          const account = accounts.get(user.email);
          if (account) { account.name = user.name; account.avatar = user.avatar; }
          
          if (user.status === 'invisible') broadcast({ type: 'user_leave', userId: userId }, userId);
          else broadcast({ type: 'user_update', user: { id: userId, ...user } }, userId);
          break;
        }

        case 'update_settings': {
          const user = users.get(userId);
          if (!user) return;
          
          const account = accounts.get(user.email);
          if (account) {
            account.settings = { ...account.settings, ...data.settings };
            ws.send(JSON.stringify({ type: 'settings_updated', settings: account.settings }));
          }
          break;
        }

        case 'typing': {
          broadcastToServer(data.serverId, { type: 'typing', userId: userId, serverId: data.serverId, channel: data.channel, name: users.get(userId)?.name }, userId);
          break;
        }

        case 'friend_request': {
          // Find user by name
          let targetId = null;
          users.forEach((u, id) => {
            if (u.name.toLowerCase() === data.name.toLowerCase()) targetId = id;
          });
          
          if (!targetId) {
            ws.send(JSON.stringify({ type: 'friend_error', message: 'Пользователь не найден' }));
            return;
          }
          if (targetId === userId) {
            ws.send(JSON.stringify({ type: 'friend_error', message: 'Нельзя добавить себя' }));
            return;
          }
          
          const myFriends = friends.get(userId) || new Set();
          if (myFriends.has(targetId)) {
            ws.send(JSON.stringify({ type: 'friend_error', message: 'Уже в друзьях' }));
            return;
          }
          
          if (!friendRequests.has(targetId)) friendRequests.set(targetId, new Set());
          friendRequests.get(targetId).add(userId);
          
          sendTo(targetId, { type: 'friend_request_incoming', from: userId, user: users.get(userId) });
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
          
          ws.send(JSON.stringify({ type: 'friend_added', user: { id: fromId, ...users.get(fromId) } }));
          sendTo(fromId, { type: 'friend_added', user: { id: userId, ...users.get(userId) } });
          break;
        }

        case 'friend_reject': {
          const requests = friendRequests.get(userId);
          if (requests) requests.delete(data.from);
          break;
        }

        case 'friend_remove': {
          const myFriends = friends.get(userId);
          const theirFriends = friends.get(data.oderId);
          if (myFriends) myFriends.delete(data.oderId);
          if (theirFriends) theirFriends.delete(userId);
          
          ws.send(JSON.stringify({ type: 'friend_removed', oderId: data.oderId }));
          sendTo(data.oderId, { type: 'friend_removed', oderId: userId });
          break;
        }

        case 'get_friends': {
          const myFriends = friends.get(userId) || new Set();
          const friendList = [...myFriends].map(id => ({ id, ...users.get(id) })).filter(f => f.name);
          const requests = friendRequests.get(userId) || new Set();
          const requestList = [...requests].map(id => ({ id, ...users.get(id) })).filter(f => f.name);
          
          ws.send(JSON.stringify({ type: 'friends_list', friends: friendList, requests: requestList }));
          break;
        }

        case 'get_server_members': {
          const srv = servers.get(data.serverId);
          if (!srv) return;
          
          const memberList = [...srv.members].map(id => {
            const u = users.get(id);
            return u ? { id, ...u, role: srv.memberRoles[id], isOwner: srv.ownerId === id } : null;
          }).filter(Boolean);
          
          ws.send(JSON.stringify({ type: 'server_members', serverId: data.serverId, members: memberList }));
          break;
        }
      }
    } catch (e) {
      console.error('Error:', e);
    }
  });

  ws.on('close', () => {
    if (userId) {
      const user = users.get(userId);
      const voiceData = voiceChannels.get(userId);
      if (voiceData) {
        voiceChannels.delete(userId);
        broadcastToServer(voiceData.serverId, { type: 'voice_state_update', serverId: voiceData.serverId, channelId: voiceData.channelId, users: getVoiceUsers(voiceData.serverId, voiceData.channelId) });
      }
      users.delete(userId);
      if (user?.status !== 'invisible') broadcast({ type: 'user_leave', userId: userId });
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Ruscord server on port ${PORT}`));
