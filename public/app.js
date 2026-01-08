// Config
const WS_URL = 'wss://discord-clone-ws.onrender.com';
// const WS_URL = 'ws://localhost:3001';

// State
const state = {
  ws: null,
  userId: null,
  user: { name: '', avatar: null, status: 'online' },
  settings: { theme: 'dark', micDevice: 'default', speakerDevice: 'default' },
  users: new Map(),
  servers: new Map(),
  dmMessages: {},
  currentServer: null,
  currentChannel: null,
  currentDM: null,
  voiceChannel: null,
  voiceUsers: [],
  micMuted: false,
  soundMuted: false,
  contextServer: null,
  contextMember: null,
  creatingVoice: false,
  localStream: null,
  peerConnections: new Map(),
  inCall: false,
  callTarget: null,
  friends: new Map(),
  friendRequests: [],
  serverMembers: []
};

// Utils
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);
const getInitial = n => n ? n[0].toUpperCase() : '?';

function setAvatar(el, avatar, name) {
  if (avatar) {
    el.style.backgroundImage = `url(${avatar})`;
    el.textContent = '';
  } else {
    el.style.backgroundImage = '';
    el.textContent = getInitial(name);
  }
}

function escapeHtml(t) {
  const d = document.createElement('div');
  d.textContent = t || '';
  return d.innerHTML;
}

function formatSize(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(1) + ' MB';
}

function getStatus(s) {
  return { online: 'В сети', idle: 'Не активен', dnd: 'Не беспокоить', invisible: 'Невидимый', offline: 'Не в сети' }[s] || 'В сети';
}

function displayStatus(s) {
  return s === 'invisible' ? 'offline' : s;
}

// WebRTC Config
const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ]
};

async function getMediaStream(video = false) {
  try {
    const constraints = {
      audio: { deviceId: state.settings.micDevice !== 'default' ? { exact: state.settings.micDevice } : undefined },
      video: video
    };
    state.localStream = await navigator.mediaDevices.getUserMedia(constraints);
    return state.localStream;
  } catch (e) {
    console.error('Media error:', e);
    return null;
  }
}

function createPeerConnection(oderId) {
  const pc = new RTCPeerConnection(rtcConfig);
  
  pc.onicecandidate = e => {
    if (e.candidate) {
      send({ type: state.voiceChannel ? 'voice_signal' : 'call_signal', to: oderId, signal: { ice: e.candidate } });
    }
  };
  
  pc.ontrack = e => {
    let audio = document.getElementById('remote-audio-' + oderId);
    if (!audio) {
      audio = document.createElement('audio');
      audio.id = 'remote-audio-' + oderId;
      audio.autoplay = true;
      document.body.appendChild(audio);
    }
    audio.srcObject = e.streams[0];
  };
  
  if (state.localStream) {
    state.localStream.getTracks().forEach(track => pc.addTrack(track, state.localStream));
  }
  
  state.peerConnections.set(oderId, pc);
  return pc;
}

async function startCall(oderId, video = false) {
  state.inCall = true;
  state.callTarget = oderId;
  
  await getMediaStream(video);
  const pc = createPeerConnection(oderId);
  
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  
  send({ type: 'call_start', to: oderId, callType: video ? 'video' : 'audio' });
  send({ type: 'call_signal', to: oderId, signal: { sdp: pc.localDescription } });
}

async function acceptCall(callerId) {
  state.inCall = true;
  state.callTarget = callerId;
  
  await getMediaStream(false);
  send({ type: 'call_accept', from: callerId });
}

function endCall() {
  if (state.callTarget) {
    send({ type: 'call_end', to: state.callTarget });
  }
  cleanupCall();
}

function cleanupCall() {
  state.peerConnections.forEach(pc => pc.close());
  state.peerConnections.clear();
  if (state.localStream) {
    state.localStream.getTracks().forEach(t => t.stop());
    state.localStream = null;
  }
  state.inCall = false;
  state.callTarget = null;
  $$('audio[id^="remote-audio"]').forEach(a => a.remove());
}


// WebSocket
function connect() {
  state.ws = new WebSocket(WS_URL);
  
  state.ws.onopen = () => {
    console.log('Connected');
    const err = $('#auth-error');
    if (err) err.textContent = '';
  };
  
  state.ws.onmessage = e => {
    try {
      handleMessage(JSON.parse(e.data));
    } catch (err) {
      console.error(err);
    }
  };
  
  state.ws.onclose = () => {
    console.log('Disconnected, reconnecting...');
    setTimeout(connect, 3000);
  };
  state.ws.onerror = err => console.error('WS Error:', err);
}

function send(data) {
  if (state.ws?.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(data));
    return true;
  }
  return false;
}

async function handleMessage(data) {
  console.log('MSG:', data.type);
  
  switch (data.type) {
    case 'auth_error':
      $('#auth-error').textContent = data.message;
      break;
      
    case 'auth_success':
      state.userId = data.userId;
      state.user = data.user;
      state.settings = data.settings || state.settings;
      
      Object.entries(data.servers || {}).forEach(([id, s]) => state.servers.set(id, s));
      (data.users || []).forEach(u => state.users.set(u.id, u));
      
      closeModal('auth-modal');
      applyTheme(state.settings.theme);
      renderAll();
      
      // Load friends
      send({ type: 'get_friends' });
      break;
      
    case 'user_join':
      state.users.set(data.user.id, data.user);
      renderUsers();
      renderMembers();
      break;
      
    case 'user_leave':
      state.users.delete(data.userId);
      renderUsers();
      renderMembers();
      break;
      
    case 'user_update':
      state.users.set(data.user.id, data.user);
      renderUsers();
      renderMembers();
      renderDMList();
      break;
      
    case 'message':
      const srv = state.servers.get(data.serverId);
      if (srv) {
        if (!srv.messages[data.channel]) srv.messages[data.channel] = [];
        srv.messages[data.channel].push(data.message);
        if (state.currentServer === data.serverId && state.currentChannel === data.channel) {
          renderMessages();
        }
      }
      break;
      
    case 'dm':
      if (!state.dmMessages[data.message.from]) state.dmMessages[data.message.from] = [];
      state.dmMessages[data.message.from].push(data.message);
      if (state.currentDM === data.message.from) renderDMMessages();
      renderDMList();
      break;
      
    case 'dm_sent':
      if (!state.dmMessages[data.to]) state.dmMessages[data.to] = [];
      state.dmMessages[data.to].push(data.message);
      if (state.currentDM === data.to) renderDMMessages();
      break;
      
    case 'server_created':
    case 'server_joined':
      state.servers.set(data.server.id, data.server);
      renderServers();
      if (data.type === 'server_created') {
        closeModal('create-server-modal');
        switchToServer(data.server.id);
      }
      break;
      
    case 'server_left':
      state.servers.delete(data.serverId);
      renderServers();
      switchToHome();
      break;
      
    case 'server_updated':
      const s = state.servers.get(data.serverId);
      if (s) {
        s.name = data.name;
        s.icon = data.icon;
        renderServers();
        if (state.currentServer === data.serverId) $('#server-name').textContent = data.name;
      }
      closeModal('server-settings-modal');
      break;
      
    case 'channel_created':
      const serv = state.servers.get(data.serverId);
      if (serv) {
        if (data.isVoice) serv.voiceChannels.push(data.channel);
        else {
          serv.channels.push(data.channel);
          serv.messages[data.channel.id] = [];
        }
        if (state.currentServer === data.serverId) renderChannels();
      }
      closeModal('channel-modal');
      break;
      
    case 'invite_created':
      $('#invite-code-display').value = data.code;
      openModal('invite-modal');
      break;
      
    case 'invite_error':
      $('#invite-error').textContent = data.message;
      break;
      
    case 'member_joined':
      if (data.user) state.users.set(data.user.id, data.user);
      renderMembers();
      break;
      
    case 'member_left':
      renderMembers();
      break;
      
    case 'voice_state_update':
      state.voiceUsers = data.users || [];
      renderVoiceUsers();
      renderVoiceInChannel(data.serverId, data.channelId);
      // Connect to new voice users
      for (const u of state.voiceUsers) {
        if (u.oderId !== state.userId && !state.peerConnections.has(u.oderId)) {
          await connectToVoiceUser(u.oderId);
        }
      }
      break;
      
    case 'voice_signal':
      await handleVoiceSignal(data.from, data.signal);
      break;
      
    case 'voice_speaking_update':
      // Update speaking animation for other users
      const speakingAvatar = $(`.voice-user[data-user-id="${data.oderId}"] .avatar`);
      if (speakingAvatar) {
        speakingAvatar.classList.toggle('speaking', data.speaking);
      }
      break;
      
    case 'call_incoming':
      showIncomingCall(data.from, data.user, data.callType);
      break;
      
    case 'call_accepted':
      console.log('Call accepted');
      break;
      
    case 'call_rejected':
      cleanupCall();
      break;
      
    case 'call_ended':
      cleanupCall();
      break;
      
    case 'call_signal':
      await handleCallSignal(data.from, data.signal);
      break;
      
    case 'settings_updated':
      state.settings = data.settings;
      applyTheme(state.settings.theme);
      break;
      
    case 'typing':
      if (data.serverId === state.currentServer && data.channel === state.currentChannel) {
        showTyping(data.name);
      }
      break;
      
    case 'friends_list':
      state.friends.clear();
      (data.friends || []).forEach(f => state.friends.set(f.id, f));
      state.friendRequests = data.requests || [];
      renderFriends();
      break;
      
    case 'friend_added':
      state.friends.set(data.user.id, data.user);
      renderFriends();
      break;
      
    case 'friend_removed':
      state.friends.delete(data.oderId);
      renderFriends();
      break;
      
    case 'friend_request_incoming':
      state.friendRequests.push(data.user);
      renderFriends();
      break;
      
    case 'friend_error':
      alert(data.message);
      break;
      
    case 'server_members':
      state.serverMembers = data.members || [];
      renderMembers();
      break;
  }
}

async function connectToVoiceUser(oderId) {
  const pc = createPeerConnection(oderId);
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  send({ type: 'voice_signal', to: oderId, signal: { sdp: pc.localDescription } });
}

async function handleVoiceSignal(from, signal) {
  let pc = state.peerConnections.get(from);
  if (!pc) pc = createPeerConnection(from);
  
  if (signal.sdp) {
    await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
    if (signal.sdp.type === 'offer') {
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      send({ type: 'voice_signal', to: from, signal: { sdp: pc.localDescription } });
    }
  }
  if (signal.ice) {
    await pc.addIceCandidate(new RTCIceCandidate(signal.ice));
  }
}

async function handleCallSignal(from, signal) {
  let pc = state.peerConnections.get(from);
  if (!pc) pc = createPeerConnection(from);
  
  if (signal.sdp) {
    await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
    if (signal.sdp.type === 'offer') {
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      send({ type: 'call_signal', to: from, signal: { sdp: pc.localDescription } });
    }
  }
  if (signal.ice) {
    await pc.addIceCandidate(new RTCIceCandidate(signal.ice));
  }
}

function showIncomingCall(from, user, callType) {
  // Simple confirm for now
  if (confirm(`${user?.name || 'Пользователь'} звонит вам. Принять?`)) {
    acceptCall(from);
  } else {
    send({ type: 'call_reject', from });
  }
}


// Render
function renderAll() {
  updateUserPanel();
  renderServers();
  renderUsers();
  renderDMList();
}

function renderServers() {
  const container = $('#servers-list');
  container.querySelectorAll('.server-btn[data-server]').forEach(el => el.remove());
  
  const addBtn = $('#add-server-btn');
  state.servers.forEach((server, id) => {
    // Show server if user is member
    if (!server.isMember && !server.members?.includes(state.userId)) return;
    
    const btn = document.createElement('div');
    btn.className = `server-btn ${state.currentServer === id ? 'active' : ''}`;
    btn.dataset.server = id;
    btn.title = server.name;
    
    if (server.icon) {
      btn.style.backgroundImage = `url(${server.icon})`;
    } else {
      btn.textContent = getInitial(server.name);
    }
    
    btn.onclick = () => switchToServer(id);
    btn.oncontextmenu = e => showServerContext(e, id);
    
    container.insertBefore(btn, addBtn);
  });
}

function renderUsers() {
  // Show only friends
  const online = [], offline = [];
  
  state.friends.forEach((u, id) => {
    const ds = displayStatus(u.status || 'offline');
    if (ds !== 'offline') online.push({ id, ...u, ds });
    else offline.push({ id, ...u, ds });
  });
  
  $('#online-users').innerHTML = online.length ? online.map(userItemHTML).join('') : '<div class="empty">Нет друзей в сети</div>';
  $('#all-users').innerHTML = state.friends.size ? [...state.friends.values()].map(u => userItemHTML({ ...u, ds: displayStatus(u.status || 'offline') })).join('') : '<div class="empty">Нет друзей</div>';
  
  bindUserActions();
}

function renderFriends() {
  renderUsers();
  
  // Render friend requests
  const requestsHTML = state.friendRequests.map(u => `
    <div class="user-item request" data-id="${u.id}">
      <div class="avatar" ${u.avatar ? `style="background-image:url(${u.avatar})"` : ''}>${u.avatar ? '' : getInitial(u.name)}</div>
      <div class="info">
        <div class="name">${escapeHtml(u.name)}</div>
        <div class="status">Хочет добавить вас в друзья</div>
      </div>
      <div class="actions">
        <button class="accept-btn" data-id="${u.id}" title="Принять">
          <svg viewBox="0 0 24 24"><path fill="currentColor" d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
        </button>
        <button class="reject-btn" data-id="${u.id}" title="Отклонить">
          <svg viewBox="0 0 24 24"><path fill="currentColor" d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>
        </button>
      </div>
    </div>
  `).join('');
  
  const requestsContainer = $('#friend-requests');
  if (requestsContainer) {
    requestsContainer.innerHTML = requestsHTML || '<div class="empty">Нет заявок</div>';
    $$('.accept-btn').forEach(btn => {
      btn.onclick = () => send({ type: 'friend_accept', from: btn.dataset.id });
    });
    $$('.reject-btn').forEach(btn => {
      btn.onclick = () => send({ type: 'friend_reject', from: btn.dataset.id });
    });
  }
}

function userItemHTML(u) {
  return `
    <div class="user-item" data-id="${u.id}">
      <div class="avatar ${u.ds}" ${u.avatar ? `style="background-image:url(${u.avatar})"` : ''}>${u.avatar ? '' : getInitial(u.name)}</div>
      <div class="info">
        <div class="name">${escapeHtml(u.name)}</div>
        <div class="status">${getStatus(u.ds)}</div>
      </div>
      <div class="actions">
        <button class="msg-btn" data-id="${u.id}" title="Сообщение">
          <svg viewBox="0 0 24 24"><path fill="currentColor" d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/></svg>
        </button>
      </div>
    </div>
  `;
}

function bindUserActions() {
  $$('.msg-btn').forEach(btn => {
    btn.onclick = e => { e.stopPropagation(); openDM(btn.dataset.id); };
  });
}

function renderDMList() {
  const dms = new Map();
  Object.keys(state.dmMessages).forEach(id => {
    const u = state.users.get(id);
    if (u) dms.set(id, u);
  });
  
  $('#dm-list').innerHTML = dms.size ? [...dms.entries()].map(([id, u]) => `
    <div class="dm-item ${state.currentDM === id ? 'active' : ''}" data-id="${id}">
      <div class="avatar" ${u.avatar ? `style="background-image:url(${u.avatar})"` : ''}>${u.avatar ? '' : getInitial(u.name)}</div>
      <span>${escapeHtml(u.name)}</span>
    </div>
  `).join('') : '';
  
  $$('.dm-item').forEach(el => { el.onclick = () => openDM(el.dataset.id); });
}

function renderChannels() {
  const server = state.servers.get(state.currentServer);
  if (!server) return;
  
  $('#server-name').textContent = server.name;
  
  const isOwner = server.ownerId === state.userId;
  $('#add-channel-btn').style.display = isOwner ? 'flex' : 'none';
  $('#add-voice-btn').style.display = isOwner ? 'flex' : 'none';
  
  $('#channel-list').innerHTML = (server.channels || []).map(c => `
    <div class="channel-item ${state.currentChannel === c.id ? 'active' : ''}" data-id="${c.id}">
      <svg viewBox="0 0 24 24"><path fill="currentColor" d="M5.41 21L6.12 17H2V15H6.53L7.24 11H3V9H7.65L8.36 5H10.36L9.65 9H13.65L14.36 5H16.36L15.65 9H20V11H15.24L14.53 15H19V17H14.12L13.41 21H11.41L12.12 17H8.12L7.41 21H5.41Z"/></svg>
      ${escapeHtml(c.name)}
    </div>
  `).join('');
  
  $('#voice-list').innerHTML = (server.voiceChannels || []).map(v => {
    const users = state.voiceUsers.filter(u => u.oderId === v.id);
    const connected = state.voiceChannel === v.id;
    return `
      <div class="voice-item ${connected ? 'connected' : ''}" data-id="${v.id}">
        <svg viewBox="0 0 24 24"><path fill="currentColor" d="M3 9v6h4l5 5V4L7 9H3z"/></svg>
        ${escapeHtml(v.name)}
      </div>
      ${users.length ? `<div class="voice-users-list">${users.map(u => `
        <div class="voice-user-item">
          <div class="avatar" ${u.avatar ? `style="background-image:url(${u.avatar})"` : ''}>${u.avatar ? '' : getInitial(u.name)}</div>
          ${escapeHtml(u.name)}
        </div>
      `).join('')}</div>` : ''}
    `;
  }).join('');
  
  $$('.channel-item').forEach(el => { el.onclick = () => openChannel(el.dataset.id); });
  $$('.voice-item').forEach(el => { el.onclick = () => joinVoice(el.dataset.id); });
}

function renderMembers() {
  const server = state.servers.get(state.currentServer);
  if (!server) return;
  
  // Request server members
  send({ type: 'get_server_members', serverId: state.currentServer });
  
  const online = [], offline = [];
  const members = state.serverMembers.length ? state.serverMembers : 
    (server.members || []).map(id => {
      if (id === state.userId) return { id, ...state.user, isOwner: server.ownerId === id };
      const u = state.users.get(id);
      return u ? { id, ...u, isOwner: server.ownerId === id } : null;
    }).filter(Boolean);
  
  members.forEach(m => {
    const s = displayStatus(m.status || 'offline');
    if (s === 'offline') offline.push({ ...m, ds: s });
    else online.push({ ...m, ds: s });
  });
  
  $('#online-count').textContent = online.length;
  $('#offline-count').textContent = offline.length;
  
  const crownSVG = '<svg class="crown" viewBox="0 0 24 24"><path fill="#f1c40f" d="M5 16L3 5l5.5 5L12 4l3.5 6L21 5l-2 11H5z"/></svg>';
  
  $('#members-online').innerHTML = online.map(m => `
    <div class="member-item" data-member-id="${m.id}">
      <div class="avatar online" ${m.avatar ? `style="background-image:url(${m.avatar})"` : ''}>${m.avatar ? '' : getInitial(m.name)}</div>
      <span>${escapeHtml(m.name)}</span>
      ${m.isOwner ? crownSVG : ''}
    </div>
  `).join('');
  
  $('#members-offline').innerHTML = offline.map(m => `
    <div class="member-item" data-member-id="${m.id}">
      <div class="avatar offline" ${m.avatar ? `style="background-image:url(${m.avatar})"` : ''}>${m.avatar ? '' : getInitial(m.name)}</div>
      <span>${escapeHtml(m.name)}</span>
      ${m.isOwner ? crownSVG : ''}
    </div>
  `).join('');
  
  // Add context menu handlers
  $$('.member-item').forEach(el => {
    el.oncontextmenu = e => showMemberContext(e, el.dataset.memberId);
  });
}

function renderMessages() {
  const server = state.servers.get(state.currentServer);
  const msgs = server?.messages?.[state.currentChannel] || [];
  
  $('#messages').innerHTML = msgs.map(messageHTML).join('');
  $('#messages').scrollTop = $('#messages').scrollHeight;
}

function renderDMMessages() {
  const msgs = state.dmMessages[state.currentDM] || [];
  const user = state.users.get(state.currentDM);
  
  $('#dm-messages').innerHTML = msgs.map(m => {
    const isMe = m.from === state.userId || m.author === state.user.name;
    const avatar = isMe ? state.user.avatar : (user?.avatar || m.avatar);
    const name = isMe ? state.user.name : (user?.name || m.author);
    return messageHTML({ ...m, avatar, author: name });
  }).join('');
  $('#dm-messages').scrollTop = $('#dm-messages').scrollHeight;
}

function messageHTML(m) {
  let fileHTML = '';
  if (m.file) {
    fileHTML = `
      <div class="file" onclick="downloadFile('${escapeHtml(m.file.name)}', '${m.file.data || ''}')">
        <svg viewBox="0 0 24 24"><path fill="currentColor" d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm4 18H6V4h7v5h5v11z"/></svg>
        <div>
          <div class="file-name">${escapeHtml(m.file.name)}</div>
          <div class="file-size">${formatSize(m.file.size || 0)}</div>
        </div>
      </div>
    `;
  }
  
  return `
    <div class="message">
      <div class="avatar" ${m.avatar ? `style="background-image:url(${m.avatar})"` : ''}>${m.avatar ? '' : getInitial(m.author)}</div>
      <div class="content">
        <div class="header">
          <span class="author">${escapeHtml(m.author)}</span>
          <span class="time">${m.time}</span>
        </div>
        <div class="text">${escapeHtml(m.text)}</div>
        ${fileHTML}
      </div>
    </div>
  `;
}

function renderVoiceUsers() {
  $('#voice-users').innerHTML = state.voiceUsers.map(u => `
    <div class="voice-user" data-user-id="${u.oderId || u.oderId}">
      <div class="avatar ${u.muted ? 'muted' : ''}" ${u.avatar ? `style="background-image:url(${u.avatar})"` : ''}>${u.avatar ? '' : getInitial(u.name)}</div>
      <span>${escapeHtml(u.name)}</span>
    </div>
  `).join('');
}

// Voice activity detection
let audioContext = null;
let analyser = null;
let speakingCheckInterval = null;

function startVoiceActivityDetection() {
  if (!state.localStream || audioContext) return;
  
  audioContext = new AudioContext();
  analyser = audioContext.createAnalyser();
  analyser.fftSize = 512;
  analyser.smoothingTimeConstant = 0.4;
  
  const source = audioContext.createMediaStreamSource(state.localStream);
  source.connect(analyser);
  
  const dataArray = new Uint8Array(analyser.frequencyBinCount);
  let wasSpeaking = false;
  
  speakingCheckInterval = setInterval(() => {
    analyser.getByteFrequencyData(dataArray);
    const average = dataArray.reduce((a, b) => a + b) / dataArray.length;
    const isSpeaking = average > 15 && !state.micMuted;
    
    // Send speaking status to server if changed
    if (isSpeaking !== wasSpeaking) {
      wasSpeaking = isSpeaking;
      send({ type: 'voice_speaking', speaking: isSpeaking });
    }
    
    // Update local user's avatar
    const myAvatar = $(`.voice-user[data-user-id="${state.userId}"] .avatar`);
    if (myAvatar) {
      myAvatar.classList.toggle('speaking', isSpeaking);
    }
  }, 100);
}

function stopVoiceActivityDetection() {
  if (speakingCheckInterval) {
    clearInterval(speakingCheckInterval);
    speakingCheckInterval = null;
  }
  if (audioContext) {
    audioContext.close();
    audioContext = null;
    analyser = null;
  }
}

function renderVoiceInChannel(serverId, channelId) {
  if (state.currentServer === serverId) renderChannels();
}

function updateUserPanel() {
  $('#user-name').textContent = state.user.name;
  $('#user-status').textContent = getStatus(state.user.status);
  setAvatar($('#user-avatar'), state.user.avatar, state.user.name);
  setAvatar($('#settings-avatar'), state.user.avatar, state.user.name);
  $('#settings-name').value = state.user.name;
  $('#settings-status').value = state.user.status;
}


// Views
function showView(id) {
  $$('.main-view').forEach(v => v.classList.remove('active'));
  $(`#${id}`).classList.add('active');
  $('#members-panel').classList.toggle('visible', id === 'chat-view');
}

function showSidebar(id) {
  $$('.sidebar-view').forEach(v => v.classList.remove('active'));
  $(`#${id}`).classList.add('active');
}

function switchToHome() {
  state.currentServer = null;
  state.currentChannel = null;
  
  $$('.server-btn').forEach(b => b.classList.remove('active'));
  $('.home-btn').classList.add('active');
  
  showSidebar('home-view');
  showView('friends-view');
}

function switchToServer(serverId) {
  state.currentServer = serverId;
  state.currentDM = null;
  
  $$('.server-btn').forEach(b => b.classList.remove('active'));
  $(`.server-btn[data-server="${serverId}"]`)?.classList.add('active');
  
  showSidebar('server-view');
  renderChannels();
  renderMembers();
  
  const server = state.servers.get(serverId);
  if (server?.channels?.length) openChannel(server.channels[0].id);
}

function openChannel(channelId) {
  state.currentChannel = channelId;
  state.currentDM = null;
  
  const server = state.servers.get(state.currentServer);
  const channel = server?.channels?.find(c => c.id === channelId);
  
  $('#channel-name').textContent = channel?.name || 'канал';
  $('#msg-input').placeholder = `Написать в #${channel?.name || 'канал'}`;
  
  renderChannels();
  showView('chat-view');
  renderMessages();
}

function openDM(oderId) {
  state.currentDM = oderId;
  state.currentChannel = null;
  state.currentServer = null;
  
  const user = state.users.get(oderId);
  const name = user?.name || 'Пользователь';
  
  $('#dm-header-name').textContent = name;
  setAvatar($('#dm-header-avatar'), user?.avatar, name);
  $('#dm-name').textContent = name;
  $('#dm-username').textContent = name.toLowerCase().replace(/\s/g, '');
  $('#dm-name-hint').textContent = name;
  setAvatar($('#dm-avatar'), user?.avatar, name);
  $('#dm-input').placeholder = `Написать @${name}`;
  
  $$('.server-btn').forEach(b => b.classList.remove('active'));
  $('.home-btn').classList.add('active');
  showSidebar('home-view');
  
  renderDMList();
  showView('dm-view');
  renderDMMessages();
}

async function joinVoice(channelId) {
  if (state.voiceChannel === channelId) {
    leaveVoice();
    return;
  }
  
  if (state.voiceChannel) leaveVoice();
  
  await getMediaStream(false);
  state.voiceChannel = channelId;
  send({ type: 'voice_join', serverId: state.currentServer, channelId });
  
  const server = state.servers.get(state.currentServer);
  const channel = server?.voiceChannels?.find(v => v.id === channelId);
  $('#voice-name').textContent = channel?.name || 'Голосовой';
  
  startVoiceActivityDetection();
  renderChannels();
  showView('voice-view');
}

function leaveVoice() {
  stopVoiceActivityDetection();
  send({ type: 'voice_leave' });
  state.voiceChannel = null;
  state.voiceUsers = [];
  
  state.peerConnections.forEach(pc => pc.close());
  state.peerConnections.clear();
  if (state.localStream) {
    state.localStream.getTracks().forEach(t => t.stop());
    state.localStream = null;
  }
  $$('audio[id^="remote-audio"]').forEach(a => a.remove());
  
  renderChannels();
  if (state.currentChannel) showView('chat-view');
  else showView('friends-view');
}

// Actions
function sendMessage() {
  const input = $('#msg-input');
  const text = input.value.trim();
  if (!text || !state.currentServer || !state.currentChannel) return;
  
  send({ type: 'message', serverId: state.currentServer, channel: state.currentChannel, text });
  input.value = '';
}

function sendDM() {
  const input = $('#dm-input');
  const text = input.value.trim();
  if (!text || !state.currentDM) return;
  
  send({ type: 'dm', to: state.currentDM, text });
  input.value = '';
}

function attachFile(isDM = false) {
  const input = isDM ? $('#dm-file-input') : $('#file-input');
  input.onchange = e => {
    const file = e.target.files[0];
    if (!file) return;
    
    const reader = new FileReader();
    reader.onload = () => {
      const fileData = { name: file.name, size: file.size, data: reader.result };
      if (isDM) send({ type: 'dm', to: state.currentDM, text: '', file: fileData });
      else send({ type: 'message', serverId: state.currentServer, channel: state.currentChannel, text: '', file: fileData });
    };
    reader.readAsDataURL(file);
  };
  input.click();
}

function downloadFile(name, data) {
  if (!data) return;
  const a = document.createElement('a');
  a.href = data;
  a.download = name;
  a.click();
}

function showTyping(name) {
  const el = $('#typing');
  el.textContent = `${name} печатает...`;
  clearTimeout(el.timeout);
  el.timeout = setTimeout(() => el.textContent = '', 3000);
}

function showServerContext(e, serverId) {
  e.preventDefault();
  state.contextServer = serverId;
  
  const menu = $('#server-context');
  const server = state.servers.get(serverId);
  
  menu.querySelector('[data-action="settings"]').style.display = server?.ownerId === state.userId ? 'flex' : 'none';
  menu.querySelector('[data-action="leave"]').style.display = server?.ownerId !== state.userId ? 'flex' : 'none';
  
  menu.style.left = e.clientX + 'px';
  menu.style.top = e.clientY + 'px';
  menu.classList.add('visible');
}

function showMemberContext(e, memberId) {
  e.preventDefault();
  if (memberId === state.userId) return; // Don't show menu for self
  
  state.contextMember = memberId;
  const menu = $('#member-context');
  const server = state.servers.get(state.currentServer);
  const isOwner = server?.ownerId === state.userId;
  const isMemberOwner = server?.ownerId === memberId;
  
  // Show kick button only if current user is owner and target is not owner
  const showKick = isOwner && !isMemberOwner;
  $('#member-kick-btn').style.display = showKick ? 'flex' : 'none';
  $('#member-kick-divider').style.display = showKick ? 'block' : 'none';
  
  menu.style.left = e.clientX + 'px';
  menu.style.top = e.clientY + 'px';
  menu.classList.add('visible');
}

function hideContextMenu() {
  $('#server-context').classList.remove('visible');
  $('#member-context').classList.remove('visible');
}

function openModal(id) { $(`#${id}`).classList.add('active'); }
function closeModal(id) { $(`#${id}`).classList.remove('active'); }

function applyTheme(theme) {
  document.body.className = `theme-${theme}`;
  $$('.theme-btn').forEach(b => b.classList.toggle('active', b.dataset.theme === theme));
}

async function loadAudioDevices() {
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    const mics = devices.filter(d => d.kind === 'audioinput');
    const speakers = devices.filter(d => d.kind === 'audiooutput');
    
    $('#mic-select').innerHTML = mics.map(d => `<option value="${d.deviceId}">${d.label || 'Микрофон'}</option>`).join('');
    $('#speaker-select').innerHTML = speakers.map(d => `<option value="${d.deviceId}">${d.label || 'Динамик'}</option>`).join('');
    
    if (state.settings.micDevice) $('#mic-select').value = state.settings.micDevice;
    if (state.settings.speakerDevice) $('#speaker-select').value = state.settings.speakerDevice;
  } catch (e) {
    console.error('Audio devices error:', e);
  }
}


// Init
function init() {
  // Auth
  $('#login-btn').onclick = () => {
    const email = $('#login-email').value.trim();
    const pass = $('#login-pass').value;
    if (!email || !pass) { $('#auth-error').textContent = 'Заполните все поля'; return; }
    if (!send({ type: 'login', email, password: pass })) {
      $('#auth-error').textContent = 'Нет соединения с сервером';
    }
  };
  
  $('#reg-btn').onclick = () => {
    const name = $('#reg-name').value.trim();
    const email = $('#reg-email').value.trim();
    const pass = $('#reg-pass').value;
    if (!name || !email || !pass) { $('#auth-error').textContent = 'Заполните все поля'; return; }
    if (!send({ type: 'register', name, email, password: pass })) {
      $('#auth-error').textContent = 'Нет соединения с сервером';
    }
  };
  
  $$('.modal-tab').forEach(tab => {
    tab.onclick = () => {
      $$('.modal-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      $$('.auth-tab').forEach(t => t.classList.remove('active'));
      $(`#auth-${tab.dataset.tab}`).classList.add('active');
    };
  });
  
  // Navigation
  $('.home-btn').onclick = switchToHome;
  $('#friends-btn').onclick = () => showView('friends-view');
  
  // Tabs
  $$('.tab').forEach(tab => {
    tab.onclick = () => {
      $$('.tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      $$('.tab-content').forEach(c => c.classList.remove('active'));
      $(`#tab-${tab.dataset.tab}`).classList.add('active');
    };
  });
  
  // Messages
  $('#msg-input').onkeypress = e => { if (e.key === 'Enter') sendMessage(); };
  $('#send-btn').onclick = sendMessage;
  $('#attach-btn').onclick = () => attachFile(false);
  
  $('#dm-input').onkeypress = e => { if (e.key === 'Enter') sendDM(); };
  $('#dm-send-btn').onclick = sendDM;
  $('#dm-attach-btn').onclick = () => attachFile(true);
  
  // User controls
  $('#mic-toggle').onclick = () => {
    state.micMuted = !state.micMuted;
    $('#mic-toggle').classList.toggle('muted', state.micMuted);
    if (state.localStream) {
      state.localStream.getAudioTracks().forEach(t => t.enabled = !state.micMuted);
    }
    if (state.voiceChannel) send({ type: 'voice_mute', muted: state.micMuted });
  };
  
  $('#sound-toggle').onclick = () => {
    state.soundMuted = !state.soundMuted;
    $('#sound-toggle').classList.toggle('muted', state.soundMuted);
    $$('audio[id^="remote-audio"]').forEach(a => a.muted = state.soundMuted);
  };
  
  // Voice
  $('#voice-mic').onclick = () => {
    state.micMuted = !state.micMuted;
    $('#voice-mic').classList.toggle('active', !state.micMuted);
    if (state.localStream) {
      state.localStream.getAudioTracks().forEach(t => t.enabled = !state.micMuted);
    }
    send({ type: 'voice_mute', muted: state.micMuted });
  };
  $('#voice-leave').onclick = leaveVoice;
  
  // Settings
  $('#settings-btn').onclick = () => {
    loadAudioDevices();
    openModal('settings-modal');
  };
  
  $$('.settings-tab').forEach(tab => {
    tab.onclick = () => {
      $$('.settings-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      $$('.settings-panel').forEach(p => p.classList.remove('active'));
      $(`#settings-${tab.dataset.settings}`).classList.add('active');
    };
  });
  
  $('#upload-avatar').onclick = () => $('#avatar-input').click();
  $('#avatar-input').onchange = e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      state.user.avatar = ev.target.result;
      setAvatar($('#settings-avatar'), state.user.avatar, state.user.name);
    };
    reader.readAsDataURL(file);
  };
  
  $('#remove-avatar').onclick = () => {
    state.user.avatar = null;
    setAvatar($('#settings-avatar'), null, state.user.name);
  };
  
  $('#save-profile').onclick = () => {
    state.user.name = $('#settings-name').value.trim() || 'Пользователь';
    state.user.status = $('#settings-status').value;
    send({ type: 'update_profile', name: state.user.name, avatar: state.user.avatar, status: state.user.status });
    updateUserPanel();
    closeModal('settings-modal');
  };
  
  $('#save-audio').onclick = () => {
    state.settings.micDevice = $('#mic-select').value;
    state.settings.speakerDevice = $('#speaker-select').value;
    send({ type: 'update_settings', settings: state.settings });
  };
  
  $$('.theme-btn').forEach(btn => {
    btn.onclick = () => {
      state.settings.theme = btn.dataset.theme;
      applyTheme(btn.dataset.theme);
      send({ type: 'update_settings', settings: state.settings });
    };
  });
  
  // Create server
  $('#add-server-btn').onclick = () => {
    state.newServerIcon = null;
    setAvatar($('#new-server-icon'), null, '+');
    $('#new-server-name').value = '';
    openModal('create-server-modal');
  };
  
  $('#upload-server-icon').onclick = () => $('#server-icon-input').click();
  $('#server-icon-input').onchange = e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      state.newServerIcon = ev.target.result;
      $('#new-server-icon').style.backgroundImage = `url(${ev.target.result})`;
      $('#new-server-icon').textContent = '';
    };
    reader.readAsDataURL(file);
  };
  
  $('#create-server-btn').onclick = () => {
    const name = $('#new-server-name').value.trim();
    if (!name) return;
    send({ type: 'create_server', name, icon: state.newServerIcon });
  };
  
  // Join server
  $('#join-server-btn').onclick = () => openModal('join-modal');
  $('#use-invite-btn').onclick = () => {
    const code = $('#invite-code').value.trim();
    if (!code) return;
    send({ type: 'use_invite', code });
    closeModal('join-modal');
  };
  
  // Server context menu
  document.onclick = hideContextMenu;
  
  $('#server-context').querySelectorAll('button').forEach(btn => {
    btn.onclick = () => {
      const action = btn.dataset.action;
      const serverId = state.contextServer;
      
      if (action === 'invite') {
        send({ type: 'create_invite', serverId });
      } else if (action === 'settings') {
        const server = state.servers.get(serverId);
        $('#edit-server-name').value = server?.name || '';
        setAvatar($('#edit-server-icon'), server?.icon, server?.name);
        state.editServerIcon = server?.icon;
        openModal('server-settings-modal');
      } else if (action === 'leave') {
        send({ type: 'leave_server', serverId });
      }
      hideContextMenu();
    };
  });
  
  // Member context menu
  $('#member-context').querySelectorAll('button').forEach(btn => {
    btn.onclick = () => {
      const action = btn.dataset.action;
      const memberId = state.contextMember;
      
      if (action === 'profile') {
        // Show profile modal (can be expanded later)
        const user = state.users.get(memberId) || state.serverMembers.find(m => m.id === memberId);
        if (user) alert(`Профиль: ${user.name}`);
      } else if (action === 'message') {
        openDM(memberId);
      } else if (action === 'kick') {
        if (confirm('Выгнать пользователя с сервера?')) {
          send({ type: 'kick_member', serverId: state.currentServer, memberId });
        }
      }
      hideContextMenu();
    };
  });
  
  // Server settings
  $('#edit-server-icon-btn').onclick = () => $('#edit-icon-input').click();
  $('#edit-icon-input').onchange = e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      state.editServerIcon = ev.target.result;
      $('#edit-server-icon').style.backgroundImage = `url(${ev.target.result})`;
      $('#edit-server-icon').textContent = '';
    };
    reader.readAsDataURL(file);
  };
  
  $('#save-server-btn').onclick = () => {
    const name = $('#edit-server-name').value.trim();
    if (!name) return;
    send({ type: 'update_server', serverId: state.contextServer, name, icon: state.editServerIcon });
  };
  
  // Create channel
  $('#add-channel-btn').onclick = () => { state.creatingVoice = false; openModal('channel-modal'); };
  $('#add-voice-btn').onclick = () => { state.creatingVoice = true; openModal('channel-modal'); };
  $('#create-channel-btn').onclick = () => {
    const name = $('#new-channel-name').value.trim();
    if (!name) return;
    send({ type: 'create_channel', serverId: state.currentServer, name, isVoice: state.creatingVoice });
    $('#new-channel-name').value = '';
  };
  
  // Invite
  $('#copy-invite').onclick = () => {
    navigator.clipboard.writeText($('#invite-code-display').value);
    $('#copy-invite').textContent = 'Скопировано!';
    setTimeout(() => $('#copy-invite').textContent = 'Копировать', 2000);
  };
  
  // Close modals
  $$('[data-close]').forEach(btn => {
    btn.onclick = () => btn.closest('.modal').classList.remove('active');
  });
  
  // Search users - send friend request by name
  $('#search-btn').onclick = () => {
    const name = $('#search-input').value.trim();
    if (!name) return;
    send({ type: 'friend_request', name });
    $('#search-input').value = '';
    $('#search-results').innerHTML = '<div class="empty">Запрос отправлен!</div>';
  };
  
  // DM Call buttons
  $$('.dm-header-actions .icon-btn').forEach((btn, i) => {
    if (i === 0) btn.onclick = () => { if (state.currentDM) startCall(state.currentDM, false); };
    if (i === 1) btn.onclick = () => { if (state.currentDM) startCall(state.currentDM, true); };
  });
  
  // Connect
  connect();
  openModal('auth-modal');
}

document.addEventListener('DOMContentLoaded', init);
