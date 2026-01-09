// MrDomestos* - Discord Clone
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);

const WS_URL = location.hostname === 'localhost' ? 'ws://localhost:3001' : 'wss://discord-clone-ws.onrender.com';

const state = {
  ws: null,
  userId: null,
  username: null,
  servers: new Map(),
  friends: new Map(),
  pendingRequests: [],
  dms: new Map(),
  currentServer: null,
  currentChannel: null,
  currentDM: null,
  currentView: 'online',
  contextServer: null,
  contextMember: null,
  profileUser: null,
  creatingVoice: false,
  editServerIcon: null,
  editServerBanner: null,
  voiceChannel: null,
  voiceUsers: new Map(),
  localStream: null,
  peerConnections: new Map(),
  settings: {
    theme: 'dark',
    micDevice: null,
    speakerDevice: null,
    micVolume: 100,
    speakerVolume: 100
  }
};

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function displayStatus(s) {
  return { online: 'В сети', idle: 'Не активен', dnd: 'Не беспокоить', invisible: 'Невидимый', offline: 'Не в сети' }[s] || 'В сети';
}

function send(data) {
  if (state.ws?.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(data));
    return true;
  }
  return false;
}

function connect() {
  state.ws = new WebSocket(WS_URL);
  
  state.ws.onopen = () => console.log('Connected');
  state.ws.onclose = () => setTimeout(connect, 3000);
  state.ws.onerror = e => console.error('WS Error:', e);
  state.ws.onmessage = e => handleMessage(JSON.parse(e.data));
}

function handleMessage(msg) {
  switch (msg.type) {
    case 'auth_success':
      state.userId = msg.userId;
      state.username = msg.username;
      closeModal('auth-modal');
      if (msg.servers) msg.servers.forEach(s => state.servers.set(s.id, s));
      if (msg.friends) msg.friends.forEach(f => state.friends.set(f.id, f));
      if (msg.pendingRequests) state.pendingRequests = msg.pendingRequests;
      if (msg.dms) msg.dms.forEach(d => state.dms.set(d.oderId, d));
      renderServers();
      renderFriends();
      loadAudioDevices();
      break;
      
    case 'auth_error':
      const authErr = $('#auth-error');
      if (authErr) authErr.textContent = msg.message || 'Ошибка авторизации';
      break;
      
    case 'server_created':
    case 'server_updated':
      state.servers.set(msg.server.id, msg.server);
      renderServers();
      if (state.currentServer === msg.server.id) renderChannels();
      break;
      
    case 'server_joined':
      state.servers.set(msg.server.id, msg.server);
      renderServers();
      break;
      
    case 'server_deleted':
    case 'kicked_from_server':
      state.servers.delete(msg.serverId);
      if (state.currentServer === msg.serverId) {
        state.currentServer = null;
        state.currentChannel = null;
        showView('home-view');
      }
      renderServers();
      break;

    case 'channel_created':
    case 'channel_deleted':
      const srv = state.servers.get(msg.serverId);
      if (srv) {
        if (msg.type === 'channel_created') {
          srv.channels = srv.channels || [];
          srv.channels.push(msg.channel);
        } else {
          srv.channels = srv.channels.filter(c => c.id !== msg.channelId);
        }
        if (state.currentServer === msg.serverId) renderChannels();
      }
      break;
      
    case 'message':
      if (msg.channelId && state.currentChannel === msg.channelId) {
        appendMessage(msg);
      } else if (msg.oderId) {
        const dm = state.dms.get(msg.oderId) || { oderId: msg.oderId, messages: [] };
        dm.messages = dm.messages || [];
        dm.messages.push(msg);
        state.dms.set(msg.oderId, dm);
        if (state.currentDM === msg.oderId) appendMessage(msg, true);
      }
      break;
      
    case 'messages_history':
      if (msg.channelId) {
        const server = state.servers.get(state.currentServer);
        if (server) {
          const ch = server.channels?.find(c => c.id === msg.channelId);
          if (ch) ch.messages = msg.messages;
        }
        renderMessages();
      } else if (msg.oderId) {
        const dm = state.dms.get(msg.oderId) || { oderId: msg.oderId };
        dm.messages = msg.messages;
        state.dms.set(msg.oderId, dm);
        renderDMMessages();
      }
      break;
      
    case 'friend_request_received':
      state.pendingRequests.push(msg.from);
      renderFriends();
      break;
      
    case 'friend_added':
      state.friends.set(msg.friend.id, msg.friend);
      state.pendingRequests = state.pendingRequests.filter(r => r.id !== msg.friend.id);
      renderFriends();
      break;
      
    case 'friend_removed':
      state.friends.delete(msg.oderId);
      renderFriends();
      break;
      
    case 'friend_status':
      const friend = state.friends.get(msg.oderId);
      if (friend) {
        friend.status = msg.status;
        renderFriends();
      }
      break;
      
    case 'invite_created':
      const codeDisplay = $('#invite-code-display');
      if (codeDisplay) codeDisplay.value = msg.code;
      openModal('invite-modal');
      break;
      
    case 'user_profile':
      showUserProfileData(msg.user);
      break;
      
    case 'typing':
      showTyping(msg.userId, msg.username);
      break;
      
    case 'voice_users':
      state.voiceUsers = new Map(msg.users.map(u => [u.id, u]));
      renderVoiceUsers();
      break;
      
    case 'voice_signal':
      handleVoiceSignal(msg);
      break;
      
    case 'call_incoming':
      handleIncomingCall(msg);
      break;
      
    case 'error':
      alert(msg.message || 'Произошла ошибка');
      break;
  }
}

function showView(viewId) {
  $$('.view').forEach(v => v.classList.remove('active'));
  const view = $(`#${viewId}`);
  if (view) view.classList.add('active');
}

function openModal(id) {
  const modal = $(`#${id}`);
  if (modal) modal.classList.add('active');
}

function closeModal(id) {
  const modal = $(`#${id}`);
  if (modal) modal.classList.remove('active');
}

function hideContextMenu() {
  const serverCtx = $('#server-context');
  const memberCtx = $('#member-context');
  if (serverCtx) serverCtx.classList.remove('active');
  if (memberCtx) memberCtx.classList.remove('active');
}

// Render functions
function renderServers() {
  const container = $('#servers-list');
  if (!container) return;
  
  const homeBtn = container.querySelector('.home-btn');
  const addBtn = container.querySelector('.add-server');
  const divider = container.querySelector('.server-divider');
  
  container.querySelectorAll('.server-btn:not(.home-btn):not(.add-server)').forEach(el => el.remove());
  
  state.servers.forEach(server => {
    const btn = document.createElement('div');
    btn.className = 'server-btn';
    btn.dataset.id = server.id;
    btn.title = server.name;
    
    if (server.icon) {
      btn.innerHTML = `<img src="${server.icon}" alt="${escapeHtml(server.name)}">`;
    } else {
      btn.textContent = server.name.charAt(0).toUpperCase();
    }
    
    btn.onclick = () => openServer(server.id);
    btn.oncontextmenu = e => {
      e.preventDefault();
      state.contextServer = server.id;
      showServerContext(e.clientX, e.clientY, server);
    };
    
    container.insertBefore(btn, addBtn);
  });
}


function showServerContext(x, y, server) {
  const ctx = $('#server-context');
  if (!ctx) return;
  
  const kickBtn = ctx.querySelector('[data-action="kick"]');
  if (kickBtn) kickBtn.style.display = server.ownerId === state.userId ? 'flex' : 'none';
  
  ctx.style.left = x + 'px';
  ctx.style.top = y + 'px';
  ctx.classList.add('active');
}

function renderChannels() {
  const server = state.servers.get(state.currentServer);
  if (!server) return;
  
  const textList = $('#text-channels');
  const voiceList = $('#voice-channels');
  if (!textList || !voiceList) return;
  
  const textChannels = (server.channels || []).filter(c => !c.isVoice);
  const voiceChannels = (server.channels || []).filter(c => c.isVoice);
  
  textList.innerHTML = textChannels.map(c => `
    <div class="channel ${state.currentChannel === c.id ? 'active' : ''}" data-id="${c.id}">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 9h16M4 15h16M10 3L8 21M16 3l-2 18"/></svg>
      <span>${escapeHtml(c.name)}</span>
    </div>
  `).join('');
  
  voiceList.innerHTML = voiceChannels.map(c => `
    <div class="channel voice ${state.voiceChannel === c.id ? 'active' : ''}" data-id="${c.id}">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>
      <span>${escapeHtml(c.name)}</span>
      ${state.voiceChannel === c.id ? '<div class="voice-users-mini" id="voice-users-mini"></div>' : ''}
    </div>
  `).join('');
  
  textList.querySelectorAll('.channel').forEach(el => {
    el.onclick = () => openChannel(el.dataset.id);
  });
  
  voiceList.querySelectorAll('.channel').forEach(el => {
    el.onclick = () => joinVoiceChannel(el.dataset.id);
  });
  
  renderMembers();
}

function renderMembers() {
  const server = state.servers.get(state.currentServer);
  const list = $('#members-list');
  if (!server || !list) return;
  
  const members = server.members || [];
  
  list.innerHTML = members.map(m => `
    <div class="member" data-id="${m.id}">
      <div class="avatar ${m.status || 'offline'}">${m.avatar ? `<img src="${m.avatar}">` : m.name?.charAt(0).toUpperCase() || '?'}</div>
      <div class="member-info">
        <div class="member-name">
          ${escapeHtml(m.name || 'User')}
          ${m.id === server.ownerId ? '<svg class="crown" viewBox="0 0 24 24" fill="currentColor"><path d="M2.5 19h19v2h-19v-2zm19.57-9.36c-.21-.8-1.04-1.28-1.84-1.06l-4.23 1.12-3.47-5.46c-.42-.66-1.34-.87-2.06-.47-.72.4-.94 1.3-.52 1.96l3.47 5.46-4.23 1.12c-.8.21-1.28 1.04-1.06 1.84l1.53 5.85h12.84l1.53-5.85c.22-.8-.26-1.63-1.06-1.84l-4.23-1.12 3.47-5.46c.42-.66.2-1.56-.52-1.96-.72-.4-1.64-.19-2.06.47l-3.47 5.46-4.23-1.12c-.8-.22-1.63.26-1.84 1.06z"/></svg>' : ''}
        </div>
        <div class="member-status">${displayStatus(m.status)}</div>
      </div>
    </div>
  `).join('');
  
  list.querySelectorAll('.member').forEach(el => {
    el.oncontextmenu = e => {
      e.preventDefault();
      state.contextMember = el.dataset.id;
      showMemberContext(e.clientX, e.clientY);
    };
  });
}

function showMemberContext(x, y) {
  const ctx = $('#member-context');
  if (!ctx) return;
  
  const server = state.servers.get(state.currentServer);
  const kickBtn = ctx.querySelector('[data-action="kick"]');
  
  if (kickBtn) {
    const isOwner = server?.ownerId === state.userId;
    const isTargetOwner = state.contextMember === server?.ownerId;
    kickBtn.style.display = isOwner && !isTargetOwner ? 'flex' : 'none';
  }
  
  ctx.style.left = x + 'px';
  ctx.style.top = y + 'px';
  ctx.classList.add('active');
}

function renderFriends() {
  const online = [...state.friends.values()].filter(f => f.status === 'online');
  const onlineList = $('#online-users');
  const allList = $('#all-users');
  const pendingList = $('#pending-users');
  const pendingTab = $('.friends-tab[data-view="pending"]');
  
  if (onlineList) {
    onlineList.innerHTML = online.length 
      ? online.map(u => userItemHTML(u)).join('') 
      : '<div class="empty">Нет друзей в сети</div>';
  }
  
  if (allList) {
    allList.innerHTML = state.friends.size 
      ? [...state.friends.values()].map(u => userItemHTML(u)).join('') 
      : '<div class="empty">Нет друзей</div>';
  }
  
  if (pendingList) {
    pendingList.innerHTML = state.pendingRequests.length 
      ? state.pendingRequests.map(u => pendingItemHTML(u)).join('') 
      : '<div class="empty">Нет запросов</div>';
    
    pendingList.querySelectorAll('.accept-btn').forEach(btn => {
      btn.onclick = () => send({ type: 'accept_friend', oderId: btn.dataset.id });
    });
    pendingList.querySelectorAll('.reject-btn').forEach(btn => {
      btn.onclick = () => send({ type: 'reject_friend', oderId: btn.dataset.id });
    });
  }
  
  if (pendingTab) {
    const badge = pendingTab.querySelector('.pending-badge') || document.createElement('span');
    badge.className = 'pending-badge';
    badge.textContent = state.pendingRequests.length || '';
    badge.style.display = state.pendingRequests.length ? 'inline' : 'none';
    if (!pendingTab.querySelector('.pending-badge')) pendingTab.appendChild(badge);
  }
  
  bindUserActions();
}

function userItemHTML(u) {
  return `
    <div class="user-item" data-id="${u.id}">
      <div class="avatar ${u.status || 'offline'}">${u.avatar ? `<img src="${u.avatar}">` : u.name?.charAt(0).toUpperCase() || '?'}</div>
      <div class="user-info">
        <div class="user-name">${escapeHtml(u.name || 'User')}</div>
        <div class="user-status">${displayStatus(u.status)}</div>
      </div>
      <div class="user-actions">
        <button class="msg-btn icon-btn" data-id="${u.id}" title="Написать">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
        </button>
      </div>
    </div>
  `;
}

function pendingItemHTML(u) {
  return `
    <div class="user-item pending" data-id="${u.id}">
      <div class="avatar">${u.avatar ? `<img src="${u.avatar}">` : u.name?.charAt(0).toUpperCase() || '?'}</div>
      <div class="user-info">
        <div class="user-name">${escapeHtml(u.name || 'User')}</div>
        <div class="user-status">Хочет добавить вас в друзья</div>
      </div>
      <div class="user-actions">
        <button class="accept-btn icon-btn" data-id="${u.id}" title="Принять">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>
        </button>
        <button class="reject-btn icon-btn" data-id="${u.id}" title="Отклонить">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
        </button>
      </div>
    </div>
  `;
}

function bindUserActions() {
  $$('.msg-btn').forEach(btn => {
    btn.onclick = () => openDM(btn.dataset.id);
  });
}


function renderMessages() {
  const server = state.servers.get(state.currentServer);
  const channel = server?.channels?.find(c => c.id === state.currentChannel);
  const container = $('#messages');
  if (!container) return;
  
  const messages = channel?.messages || [];
  container.innerHTML = messages.map(m => messageHTML(m)).join('');
  container.scrollTop = container.scrollHeight;
}

function renderDMMessages() {
  const dm = state.dms.get(state.currentDM);
  const container = $('#dm-messages');
  if (!container) return;
  
  const messages = dm?.messages || [];
  container.innerHTML = messages.map(m => messageHTML(m)).join('');
  container.scrollTop = container.scrollHeight;
}

function messageHTML(m) {
  const time = new Date(m.timestamp).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
  return `
    <div class="message">
      <div class="avatar">${m.avatar ? `<img src="${m.avatar}">` : m.username?.charAt(0).toUpperCase() || '?'}</div>
      <div class="message-content">
        <div class="message-header">
          <span class="message-author">${escapeHtml(m.username || 'User')}</span>
          <span class="message-time">${time}</span>
        </div>
        <div class="message-text">${escapeHtml(m.content || '')}</div>
      </div>
    </div>
  `;
}

function appendMessage(m, isDM = false) {
  const container = isDM ? $('#dm-messages') : $('#messages');
  if (!container) return;
  
  container.insertAdjacentHTML('beforeend', messageHTML(m));
  container.scrollTop = container.scrollHeight;
}

function showTyping(userId, username) {
  const el = $('#typing-indicator');
  if (!el || userId === state.userId) return;
  
  el.textContent = `${username} печатает...`;
  el.style.display = 'block';
  
  clearTimeout(el.timeout);
  el.timeout = setTimeout(() => el.style.display = 'none', 3000);
}

// Navigation
function openServer(serverId) {
  state.currentServer = serverId;
  state.currentDM = null;
  
  const server = state.servers.get(serverId);
  const serverName = $('#server-name');
  if (serverName) serverName.textContent = server?.name || 'Сервер';
  
  $$('.server-btn').forEach(b => b.classList.toggle('active', b.dataset.id === serverId));
  
  renderChannels();
  showView('server-view');
  
  const firstChannel = server?.channels?.find(c => !c.isVoice);
  if (firstChannel) openChannel(firstChannel.id);
}

function openChannel(channelId) {
  state.currentChannel = channelId;
  
  const server = state.servers.get(state.currentServer);
  const channel = server?.channels?.find(c => c.id === channelId);
  
  const channelName = $('#channel-name');
  const msgInput = $('#msg-input');
  if (channelName) channelName.textContent = channel?.name || 'Канал';
  if (msgInput) msgInput.placeholder = `Написать в #${channel?.name || 'канал'}`;
  
  renderChannels();
  showView('chat-view');
  
  send({ type: 'get_messages', channelId });
}

function openDM(oderId) {
  state.currentDM = oderId;
  state.currentChannel = null;
  state.currentServer = null;
  
  const friend = state.friends.get(oderId);
  const name = friend?.name || 'Пользователь';
  
  const dmName = $('#dm-name');
  const dmStatus = $('#dm-status');
  const dmInput = $('#dm-input');
  const dmAvatar = $('#dm-avatar');
  
  if (dmName) dmName.textContent = name;
  if (dmStatus) dmStatus.textContent = displayStatus(friend?.status);
  if (dmInput) dmInput.placeholder = `Написать @${name}`;
  if (dmAvatar) dmAvatar.textContent = name.charAt(0).toUpperCase();
  
  $$('.server-btn').forEach(b => b.classList.remove('active'));
  $('.home-btn')?.classList.add('active');
  
  showView('dm-view');
  send({ type: 'get_dm_messages', oderId });
}

// Voice
function joinVoiceChannel(channelId) {
  if (state.voiceChannel === channelId) {
    leaveVoiceChannel();
    return;
  }
  
  if (state.voiceChannel) leaveVoiceChannel();
  
  state.voiceChannel = channelId;
  send({ type: 'join_voice', channelId, serverId: state.currentServer });
  startVoice();
  renderChannels();
}

function leaveVoiceChannel() {
  send({ type: 'leave_voice', channelId: state.voiceChannel });
  stopVoice();
  state.voiceChannel = null;
  state.voiceUsers.clear();
  renderChannels();
}

async function startVoice() {
  try {
    state.localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    setupVoiceDetection();
  } catch (e) {
    console.error('Mic error:', e);
    alert('Не удалось получить доступ к микрофону');
  }
}

function stopVoice() {
  if (state.localStream) {
    state.localStream.getTracks().forEach(t => t.stop());
    state.localStream = null;
  }
  state.peerConnections.forEach(pc => pc.close());
  state.peerConnections.clear();
}

function setupVoiceDetection() {
  if (!state.localStream) return;
  
  const ctx = new AudioContext();
  const analyser = ctx.createAnalyser();
  const source = ctx.createMediaStreamSource(state.localStream);
  source.connect(analyser);
  
  analyser.fftSize = 256;
  const data = new Uint8Array(analyser.frequencyBinCount);
  
  function check() {
    if (!state.voiceChannel) return;
    
    analyser.getByteFrequencyData(data);
    const avg = data.reduce((a, b) => a + b) / data.length;
    const speaking = avg > 30;
    
    const myVoice = $(`.voice-user[data-id="${state.userId}"]`);
    if (myVoice) myVoice.classList.toggle('speaking', speaking);
    
    requestAnimationFrame(check);
  }
  check();
}

function renderVoiceUsers() {
  const container = $('#voice-users-mini');
  if (!container) return;
  
  container.innerHTML = [...state.voiceUsers.values()].map(u => `
    <div class="voice-user ${u.speaking ? 'speaking' : ''}" data-id="${u.id}">
      <div class="avatar">${u.name?.charAt(0).toUpperCase() || '?'}</div>
      <span>${escapeHtml(u.name || 'User')}</span>
    </div>
  `).join('');
}

function handleVoiceSignal(msg) {
  // WebRTC signaling
  const { from, signal } = msg;
  
  if (signal.type === 'offer') {
    const pc = createPeerConnection(from);
    pc.setRemoteDescription(new RTCSessionDescription(signal))
      .then(() => pc.createAnswer())
      .then(answer => pc.setLocalDescription(answer))
      .then(() => send({ type: 'voice_signal', to: from, signal: pc.localDescription }));
  } else if (signal.type === 'answer') {
    const pc = state.peerConnections.get(from);
    if (pc) pc.setRemoteDescription(new RTCSessionDescription(signal));
  } else if (signal.candidate) {
    const pc = state.peerConnections.get(from);
    if (pc) pc.addIceCandidate(new RTCIceCandidate(signal));
  }
}

function createPeerConnection(oderId) {
  const pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
  
  if (state.localStream) {
    state.localStream.getTracks().forEach(t => pc.addTrack(t, state.localStream));
  }
  
  pc.onicecandidate = e => {
    if (e.candidate) send({ type: 'voice_signal', to: oderId, signal: e.candidate });
  };
  
  pc.ontrack = e => {
    const audio = new Audio();
    audio.srcObject = e.streams[0];
    audio.play();
  };
  
  state.peerConnections.set(oderId, pc);
  return pc;
}

function handleIncomingCall(msg) {
  const user = state.friends.get(msg.from);
  if (confirm(`${user?.name || 'Пользователь'} звонит вам. Принять?`)) {
    openDM(msg.from);
    // Accept call logic
  }
}


// User Profile
function showUserProfile(userId) {
  state.profileUser = userId;
  send({ type: 'get_profile', userId });
}

function showUserProfileData(user) {
  const modal = $('#profile-modal');
  if (!modal) return;
  
  const avatar = $('#profile-avatar');
  const name = $('#profile-name');
  const status = $('#profile-status');
  const banner = $('#profile-banner');
  const joined = $('#profile-joined');
  const friendBtn = $('#profile-friend-btn');
  const friendText = $('#profile-friend-text');
  
  if (avatar) avatar.textContent = user.name?.charAt(0).toUpperCase() || '?';
  if (name) name.textContent = user.name || 'Пользователь';
  if (status) status.textContent = displayStatus(user.status);
  if (banner) banner.style.background = user.banner || 'linear-gradient(135deg, #5f27cd, #5f27cd88)';
  
  if (joined) {
    const date = new Date(user.createdAt || Date.now());
    joined.textContent = date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' }) + ' г.';
  }
  
  const isFriend = state.friends.has(user.id);
  if (friendText) friendText.textContent = isFriend ? 'Удалить из друзей' : 'Добавить в друзья';
  if (friendBtn) friendBtn.dataset.action = isFriend ? 'remove' : 'add';
  
  openModal('profile-modal');
}

// Server Settings
function loadServerMembers() {
  const server = state.servers.get(state.contextServer);
  if (!server) return;
  
  const members = server.members || [];
  const roles = server.roles || [];
  const list = $('#members-manage-list');
  if (!list) return;
  
  list.innerHTML = members.map(m => `
    <div class="member-manage-item" data-id="${m.id}">
      <div class="avatar">${m.avatar ? `<img src="${m.avatar}">` : m.name?.charAt(0).toUpperCase() || '?'}</div>
      <div class="member-manage-info">
        <div class="member-name">${escapeHtml(m.name || 'User')}</div>
      </div>
      <div class="member-manage-actions">
        ${m.id !== server.ownerId && server.ownerId === state.userId ? `
          <select class="role-select" data-member="${m.id}">
            <option value="">Без роли</option>
            ${roles.map(r => `<option value="${r.id}">${r.name}</option>`).join('')}
          </select>
          <button class="btn small danger kick-member-btn" data-id="${m.id}">Кик</button>
        ` : ''}
      </div>
    </div>
  `).join('') || '<div class="empty">Нет участников</div>';
  
  $$('.kick-member-btn').forEach(btn => {
    btn.onclick = () => {
      if (confirm('Удалить пользователя с сервера?')) {
        send({ type: 'kick_member', serverId: state.contextServer, memberId: btn.dataset.id });
      }
    };
  });
}

function loadServerRoles() {
  const server = state.servers.get(state.contextServer);
  if (!server) return;
  
  const roles = server.roles || [];
  const list = $('#roles-list');
  if (!list) return;
  
  list.innerHTML = roles.map(r => `
    <div class="role-item" data-id="${r.id}">
      <div class="role-color" style="background: ${r.color}"></div>
      <div class="role-name">${escapeHtml(r.name)}</div>
      <div class="role-actions">
        <button class="delete-role-btn danger" data-id="${r.id}">Удалить</button>
      </div>
    </div>
  `).join('') || '<div class="empty">Нет ролей</div>';
  
  $$('.delete-role-btn').forEach(btn => {
    btn.onclick = () => {
      send({ type: 'delete_role', serverId: state.contextServer, roleId: btn.dataset.id });
    };
  });
}

function applyTheme(theme) {
  document.body.className = `theme-${theme}`;
  $$('.theme-btn').forEach(b => b.classList.toggle('active', b.dataset.theme === theme));
  state.settings.theme = theme;
}

async function loadAudioDevices() {
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    const mics = devices.filter(d => d.kind === 'audioinput');
    const speakers = devices.filter(d => d.kind === 'audiooutput');
    
    const micSelect = $('#mic-select');
    const speakerSelect = $('#speaker-select');
    
    if (micSelect) {
      micSelect.innerHTML = mics.map(d => `<option value="${d.deviceId}">${d.label || 'Микрофон'}</option>`).join('');
      if (state.settings.micDevice) micSelect.value = state.settings.micDevice;
    }
    
    if (speakerSelect) {
      speakerSelect.innerHTML = speakers.map(d => `<option value="${d.deviceId}">${d.label || 'Динамик'}</option>`).join('');
      if (state.settings.speakerDevice) speakerSelect.value = state.settings.speakerDevice;
    }
  } catch (e) {
    console.error('Audio devices error:', e);
  }
}

// Init
function init() {
  // Auth handlers
  const loginBtn = $('#login-btn');
  if (loginBtn) {
    loginBtn.onclick = () => {
      const email = $('#login-email')?.value.trim();
      const pass = $('#login-pass')?.value;
      const authErr = $('#auth-error');
      
      if (!email || !pass) {
        if (authErr) authErr.textContent = 'Заполните все поля';
        return;
      }
      
      if (!send({ type: 'login', email, password: pass })) {
        if (authErr) authErr.textContent = 'Нет подключения к серверу';
      }
    };
  }
  
  const regBtn = $('#reg-btn');
  if (regBtn) {
    regBtn.onclick = () => {
      const name = $('#reg-name')?.value.trim();
      const email = $('#reg-email')?.value.trim();
      const pass = $('#reg-pass')?.value;
      const authErr = $('#auth-error');
      
      if (!name || !email || !pass) {
        if (authErr) authErr.textContent = 'Заполните все поля';
        return;
      }
      
      if (!send({ type: 'register', name, email, password: pass })) {
        if (authErr) authErr.textContent = 'Нет подключения к серверу';
      }
    };
  }
  
  // Auth tabs
  $$('.auth-tab').forEach(tab => {
    tab.onclick = () => {
      $$('.auth-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      $$('.auth-form').forEach(f => f.classList.remove('active'));
      $(`#${tab.dataset.form}`)?.classList.add('active');
    };
  });
  
  // Home button
  const homeBtn = $('.home-btn');
  if (homeBtn) {
    homeBtn.onclick = () => {
      state.currentServer = null;
      state.currentChannel = null;
      state.currentDM = null;
      $$('.server-btn').forEach(b => b.classList.remove('active'));
      homeBtn.classList.add('active');
      showView('home-view');
    };
  }
  
  // Friends tabs
  $$('.friends-tab').forEach(tab => {
    tab.onclick = () => {
      $$('.friends-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      state.currentView = tab.dataset.view;
      $$('.friends-list').forEach(l => l.classList.remove('active'));
      $(`#${tab.dataset.view}-users`)?.classList.add('active');
    };
  });

  
  // Add server
  const addServerBtn = $('#add-server-btn');
  if (addServerBtn) {
    addServerBtn.onclick = () => openModal('server-modal');
  }
  
  // Create server
  const createServerBtn = $('#create-server-btn');
  if (createServerBtn) {
    createServerBtn.onclick = () => {
      const name = $('#server-name-input')?.value.trim();
      if (!name) return;
      send({ type: 'create_server', name });
      $('#server-name-input').value = '';
      closeModal('server-modal');
    };
  }
  
  // Join server
  const joinServerBtn = $('#join-server-btn');
  if (joinServerBtn) {
    joinServerBtn.onclick = () => {
      const code = $('#invite-code-input')?.value.trim();
      if (!code) return;
      send({ type: 'join_server', code });
      $('#invite-code-input').value = '';
      closeModal('server-modal');
    };
  }
  
  // Send message
  const msgInput = $('#msg-input');
  if (msgInput) {
    msgInput.onkeydown = e => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        const content = msgInput.value.trim();
        if (!content) return;
        send({ type: 'message', channelId: state.currentChannel, content });
        msgInput.value = '';
      } else {
        send({ type: 'typing', channelId: state.currentChannel });
      }
    };
  }
  
  // Send DM
  const dmInput = $('#dm-input');
  if (dmInput) {
    dmInput.onkeydown = e => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        const content = dmInput.value.trim();
        if (!content) return;
        send({ type: 'dm_message', oderId: state.currentDM, content });
        dmInput.value = '';
      }
    };
  }
  
  // Theme buttons
  $$('.theme-btn').forEach(btn => {
    btn.onclick = () => applyTheme(btn.dataset.theme);
  });
  
  // Settings button
  const settingsBtn = $('#settings-btn');
  if (settingsBtn) {
    settingsBtn.onclick = () => openModal('settings-modal');
  }
  
  // Server context menu
  const serverCtx = $('#server-context');
  if (serverCtx) {
    serverCtx.querySelectorAll('button').forEach(btn => {
      btn.onclick = () => {
        const action = btn.dataset.action;
        const serverId = state.contextServer;
        
        if (action === 'invite') {
          send({ type: 'create_invite', serverId });
        } else if (action === 'settings') {
          const server = state.servers.get(serverId);
          const editName = $('#edit-server-name');
          const previewName = $('#preview-server-name');
          const previewBanner = $('#preview-banner');
          
          if (editName) editName.value = server?.name || '';
          if (previewName) previewName.textContent = server?.name || '';
          
          state.editServerIcon = server?.icon;
          state.editServerBanner = server?.banner || '#5f27cd';
          if (previewBanner) previewBanner.style.background = `linear-gradient(135deg, ${state.editServerBanner}, ${state.editServerBanner}88)`;
          
          $$('.server-settings-tab').forEach(t => t.classList.remove('active'));
          $('.server-settings-tab[data-panel="profile"]')?.classList.add('active');
          $$('.server-settings-panel').forEach(p => p.classList.remove('active'));
          $('#panel-profile')?.classList.add('active');
          
          openModal('server-settings-modal');
        } else if (action === 'leave') {
          send({ type: 'leave_server', serverId });
        }
        hideContextMenu();
      };
    });
  }
  
  // Member context menu
  const memberCtx = $('#member-context');
  if (memberCtx) {
    memberCtx.querySelectorAll('button').forEach(btn => {
      btn.onclick = () => {
        const action = btn.dataset.action;
        const memberId = state.contextMember;
        
        if (action === 'profile') {
          showUserProfile(memberId);
        } else if (action === 'message') {
          openDM(memberId);
        } else if (action === 'kick') {
          if (confirm('Удалить пользователя с сервера?')) {
            send({ type: 'kick_member', serverId: state.currentServer, memberId });
          }
        }
        hideContextMenu();
      };
    });
  }
  
  // Profile modal buttons
  const profileMsgBtn = $('#profile-message-btn');
  if (profileMsgBtn) {
    profileMsgBtn.onclick = () => {
      if (state.profileUser) {
        openDM(state.profileUser);
        closeModal('profile-modal');
      }
    };
  }
  
  const profileFriendBtn = $('#profile-friend-btn');
  if (profileFriendBtn) {
    profileFriendBtn.onclick = () => {
      if (state.profileUser) {
        const action = profileFriendBtn.dataset.action;
        if (action === 'add') {
          send({ type: 'friend_request', to: state.profileUser });
        } else {
          send({ type: 'remove_friend', oderId: state.profileUser });
        }
      }
    };
  }
  
  // Server settings tabs
  $$('.server-settings-tab[data-panel]').forEach(tab => {
    tab.onclick = () => {
      $$('.server-settings-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      $$('.server-settings-panel').forEach(p => p.classList.remove('active'));
      $(`#panel-${tab.dataset.panel}`)?.classList.add('active');
      
      if (tab.dataset.panel === 'members') loadServerMembers();
      if (tab.dataset.panel === 'roles') loadServerRoles();
    };
  });
  
  // Save server settings
  const saveServerBtn = $('#save-server-btn');
  if (saveServerBtn) {
    saveServerBtn.onclick = () => {
      const name = $('#edit-server-name')?.value.trim();
      if (!name) return;
      send({ type: 'update_server', serverId: state.contextServer, name, icon: state.editServerIcon, banner: state.editServerBanner });
    };
  }
  
  // Delete server
  const deleteServerBtn = $('#delete-server-btn');
  if (deleteServerBtn) {
    deleteServerBtn.onclick = () => {
      if (confirm('Вы уверены что хотите удалить сервер? Это действие нельзя отменить.')) {
        send({ type: 'delete_server', serverId: state.contextServer });
        closeModal('server-settings-modal');
      }
    };
  }
  
  // Create role
  const createRoleBtn = $('#create-role-btn');
  if (createRoleBtn) {
    createRoleBtn.onclick = () => {
      const name = $('#new-role-name')?.value.trim();
      const color = $('#new-role-color')?.value;
      if (!name) return;
      send({ type: 'create_role', serverId: state.contextServer, name, color });
      $('#new-role-name').value = '';
    };
  }
  
  // Create invite from settings
  const createInviteBtn = $('#create-invite-btn');
  if (createInviteBtn) {
    createInviteBtn.onclick = () => {
      send({ type: 'create_invite', serverId: state.contextServer });
    };
  }
  
  // Create channel
  const addChannelBtn = $('#add-channel-btn');
  if (addChannelBtn) {
    addChannelBtn.onclick = () => {
      state.creatingVoice = false;
      openModal('channel-modal');
    };
  }
  
  const addVoiceBtn = $('#add-voice-btn');
  if (addVoiceBtn) {
    addVoiceBtn.onclick = () => {
      state.creatingVoice = true;
      openModal('channel-modal');
    };
  }
  
  const createChannelBtn = $('#create-channel-btn');
  if (createChannelBtn) {
    createChannelBtn.onclick = () => {
      const name = $('#new-channel-name')?.value.trim();
      if (!name) return;
      send({ type: 'create_channel', serverId: state.currentServer, name, isVoice: state.creatingVoice });
      $('#new-channel-name').value = '';
      closeModal('channel-modal');
    };
  }
  
  // Copy invite
  const copyInviteBtn = $('#copy-invite');
  if (copyInviteBtn) {
    copyInviteBtn.onclick = () => {
      const codeDisplay = $('#invite-code-display');
      if (codeDisplay) navigator.clipboard.writeText(codeDisplay.value);
      copyInviteBtn.textContent = 'Скопировано!';
      setTimeout(() => copyInviteBtn.textContent = 'Копировать', 2000);
    };
  }
  
  // Close modals
  $$('[data-close]').forEach(btn => {
    btn.onclick = () => {
      const modal = btn.closest('.modal');
      if (modal) modal.classList.remove('active');
    };
  });
  
  // Search/add friend
  const searchBtn = $('#search-btn');
  if (searchBtn) {
    searchBtn.onclick = () => {
      const name = $('#search-input')?.value.trim();
      if (!name) return;
      send({ type: 'friend_request', name });
      $('#search-input').value = '';
      const results = $('#search-results');
      if (results) results.innerHTML = '<div class="empty">Запрос отправлен!</div>';
    };
  }
  
  // Hide context menus on click outside
  document.addEventListener('click', e => {
    if (!e.target.closest('.context-menu')) hideContextMenu();
  });
  
  // Connect and show auth
  connect();
  openModal('auth-modal');
}

document.addEventListener('DOMContentLoaded', init);
