// ============ CONFIG ============
var RENDER_URL = 'wss://discord-clone-ws.onrender.com';
// Always use Render server for sync between web and desktop
var WS_URL = RENDER_URL;

// ============ STATE ============
var state = {
  ws: null,
  userId: null,
  username: null,
  userAvatar: null,
  userStatus: 'online',
  customStatus: null,
  isGuest: false,
  servers: new Map(),
  friends: new Map(),
  pendingRequests: [],
  blockedUsers: new Set(),
  currentServer: null,
  currentChannel: null,
  currentDM: null,
  dmMessages: new Map(),
  dmChats: new Set(),
  voiceChannel: null,
  voiceUsers: new Map(),
  localStream: null,
  screenStream: null,
  noiseSuppressionEnabled: true,
  videoEnabled: false,
  screenSharing: false,
  replyingTo: null,
  editingMessage: null,
  newServerIcon: null,
  editServerIcon: null,
  editingServerId: null,
  editingChannelId: null,
  editingMemberId: null,
  editingRoleId: null,
  creatingVoice: false,
  forwardingMessage: null,
  searchResults: [],
  settings: { notifications: true, sounds: true, privacy: 'everyone' }
};

// ============ UTILS ============
function qS(s) { return document.querySelector(s); }
function qSA(s) { return document.querySelectorAll(s); }

function escapeHtml(t) {
  if (!t) return '';
  var d = document.createElement('div');
  d.textContent = t;
  return d.innerHTML;
}

function displayStatus(s) {
  var map = { online: 'В сети', idle: 'Не активен', dnd: 'Не беспокоить', invisible: 'Невидимый', offline: 'Не в сети' };
  return map[s] || 'В сети';
}

var authLoadingStartTime = 0;
var pendingAuthSuccess = null;

function showAuthLoading(text) {
  var loading = qS('#auth-loading');
  if (loading) {
    var textEl = loading.querySelector('.loading-text p');
    if (textEl) textEl.textContent = text || 'Подготавливаем всё для вас...';
    loading.classList.add('visible');
    loading.classList.remove('fade-out');
    
    // Reset progress bar animation
    var progressBar = loading.querySelector('.loading-progress-bar');
    if (progressBar) {
      progressBar.style.animation = 'none';
      progressBar.offsetHeight; // Trigger reflow
      progressBar.style.animation = 'progress-fill 5s ease-out forwards, shimmer 1.5s ease-in-out infinite';
    }
    
    authLoadingStartTime = Date.now();
  }
}

function hideAuthLoading() {
  var loading = qS('#auth-loading');
  if (loading) {
    loading.classList.add('fade-out');
    setTimeout(function() {
      loading.classList.remove('visible', 'fade-out');
      // Process pending auth success
      if (pendingAuthSuccess) {
        processAuthSuccess(pendingAuthSuccess);
        pendingAuthSuccess = null;
      }
    }, 300);
  }
}

function processAuthSuccess(msg) {
  state.userId = msg.userId;
  state.username = msg.user.name;
  state.userAvatar = msg.user.avatar;
  state.userStatus = msg.user.status || 'online';
  state.customStatus = msg.user.customStatus;
  state.isGuest = msg.isGuest || false;
  
  if (msg.servers) {
    Object.values(msg.servers).forEach(function(srv) {
      state.servers.set(srv.id, srv);
    });
  }
  
  if (msg.friends) {
    msg.friends.forEach(function(f) {
      state.friends.set(f.id, f);
      state.dmChats.add(f.id);
    });
  }
  
  if (msg.pendingRequests) {
    state.pendingRequests = msg.pendingRequests;
  }
  
  localStorage.setItem('session', JSON.stringify({ 
    email: localStorage.getItem('lastEmail'), 
    pwd: localStorage.getItem('lastPwd') 
  }));
  
  qS('#auth-screen').classList.remove('active');
  qS('#main-app').classList.remove('hidden');
  
  updateUserPanel();
  renderServers();
  renderFriends();
  renderDMList();
  loadAudioDevices();
  
  if (state.servers.size > 0) {
    var firstServer = state.servers.keys().next().value;
    selectServer(firstServer);
  } else {
    showFriendsView();
  }
}

function formatTime(ts) {
  if (!ts) return '--:--';
  var d = new Date(ts);
  if (isNaN(d.getTime())) return '--:--';
  return d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}


function formatDate(ts) {
  var d = new Date(ts);
  var today = new Date();
  if (d.toDateString() === today.toDateString()) return 'Сегодня';
  var yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  if (d.toDateString() === yesterday.toDateString()) return 'Вчера';
  return d.toLocaleDateString('ru-RU');
}

// ============ WEBSOCKET ============
function send(data) {
  if (state.ws && state.ws.readyState === 1) {
    state.ws.send(JSON.stringify(data));
    return true;
  }
  return false;
}

var pingInterval = null;

function startPing() {
  stopPing();
  pingInterval = setInterval(function() {
    send({ type: 'ping' });
  }, 25000);
}

function stopPing() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
}

function showConnecting() {
  var el = qS('#connecting-overlay');
  if (!el) {
    el = document.createElement('div');
    el.id = 'connecting-overlay';
    el.innerHTML = '<div class="connecting-box"><div class="connecting-spinner"></div><div class="connecting-text">Подключение к серверу...</div><div class="connecting-hint">Первое подключение может занять до 30 секунд</div></div>';
    document.body.appendChild(el);
  }
  el.classList.add('visible');
}

function hideConnecting() {
  var el = qS('#connecting-overlay');
  if (el) el.classList.remove('visible');
}

function connect() {
  showConnecting();
  state.ws = new WebSocket(WS_URL);
  
  state.ws.onopen = function() {
    hideConnecting();
    startPing();
    tryAutoLogin();
  };
  
  state.ws.onclose = function() {
    stopPing();
    setTimeout(connect, 3000);
  };
  
  state.ws.onerror = function(e) {
    console.error('WS error', e);
  };
  
  state.ws.onmessage = function(e) {
    handleMessage(JSON.parse(e.data));
  };
}

function tryAutoLogin() {
  var email = localStorage.getItem('lastEmail');
  var pwd = localStorage.getItem('lastPwd');
  if (email && pwd) {
    send({ type: 'login', email: email, password: pwd });
  }
}


// ============ MESSAGE HANDLER ============
function handleMessage(msg) {
  var handlers = {
    pong: function() {},
    
    auth_success: function() {
      // Store auth data and let hideAuthLoading process it after 5 seconds
      pendingAuthSuccess = msg;
      hideAuthLoading();
    },
    
    auth_error: function() {
      hideAuthLoading();
      localStorage.removeItem('session');
      localStorage.removeItem('lastEmail');
      localStorage.removeItem('lastPwd');
      var loginBox = qS('#login-box');
      if (loginBox && !loginBox.classList.contains('hidden')) {
        qS('#login-error').textContent = msg.message || 'Ошибка';
      } else {
        qS('#reg-error').textContent = msg.message || 'Ошибка';
      }
    },
    
    server_created: function() {
      state.servers.set(msg.server.id, msg.server);
      renderServers();
      openServer(msg.server.id);
      closeModal('create-server-modal');
    },
    
    server_joined: function() {
      state.servers.set(msg.server.id, msg.server);
      renderServers();
      openServer(msg.server.id);
      closeModal('join-modal');
    },
    
    server_updated: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        if (msg.name) srv.name = msg.name;
        if (msg.icon !== undefined) srv.icon = msg.icon;
        if (msg.region) srv.region = msg.region;
        renderServers();
        if (state.currentServer === msg.serverId) {
          qS('#server-name').textContent = srv.name;
        }
      }
    },
    
    server_deleted: function() {
      state.servers.delete(msg.serverId);
      if (state.currentServer === msg.serverId) {
        state.currentServer = null;
        state.currentChannel = null;
        showView('friends-view');
        qS('#server-view').classList.remove('active');
        qS('#home-view').classList.add('active');
      }
      renderServers();
    },
    
    server_left: function() {
      state.servers.delete(msg.serverId);
      if (state.currentServer === msg.serverId) {
        state.currentServer = null;
        state.currentChannel = null;
        showView('friends-view');
        qS('#server-view').classList.remove('active');
        qS('#home-view').classList.add('active');
      }
      renderServers();
      if (msg.kicked) showNotification('Вы были исключены с сервера');
      if (msg.banned) showNotification('Вы были забанены на сервере');
    },
    
    channel_created: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        if (msg.isVoice) {
          srv.voiceChannels.push(msg.channel);
        } else {
          srv.channels.push(msg.channel);
          srv.messages[msg.channel.id] = [];
        }
        if (state.currentServer === msg.serverId) renderChannels();
      }
      closeModal('channel-modal');
    },
    
    channel_updated: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        var channels = msg.isVoice ? srv.voiceChannels : srv.channels;
        var ch = channels.find(function(c) { return c.id === msg.channelId; });
        if (ch) {
          ch.name = msg.name;
          
          // Update voice channel name if currently in this channel
          if (msg.isVoice && state.voiceChannel === msg.channelId) {
            var voiceNameEl = qS('#voice-name');
            if (voiceNameEl) voiceNameEl.textContent = msg.name;
          }
        }
        if (state.currentServer === msg.serverId) renderChannels();
      }
    },
    
    channel_deleted: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        if (msg.isVoice) {
          srv.voiceChannels = srv.voiceChannels.filter(function(c) { return c.id !== msg.channelId; });
        } else {
          srv.channels = srv.channels.filter(function(c) { return c.id !== msg.channelId; });
          delete srv.messages[msg.channelId];
        }
        if (state.currentChannel === msg.channelId) {
          state.currentChannel = null;
          if (srv.channels[0]) openChannel(srv.channels[0].id);
        }
        if (state.currentServer === msg.serverId) renderChannels();
      }
    },

    
    message: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        if (!srv.messages[msg.channel]) srv.messages[msg.channel] = [];
        srv.messages[msg.channel].push(msg.message);
        if (state.currentServer === msg.serverId && state.currentChannel === msg.channel) {
          appendMessage(msg.message);
        }
      }
    },
    
    message_edited: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && srv.messages[msg.channelId]) {
        var m = srv.messages[msg.channelId].find(function(x) { return x.id == msg.messageId; });
        if (m) {
          m.text = msg.text;
          m.edited = true;
          m.editedAt = msg.editedAt;
        }
        if (state.currentServer === msg.serverId && state.currentChannel === msg.channelId) {
          renderMessages(srv.messages[msg.channelId]);
        }
      }
    },
    
    message_deleted: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && srv.messages[msg.channelId]) {
        srv.messages[msg.channelId] = srv.messages[msg.channelId].filter(function(m) {
          return m.id != msg.messageId;
        });
        srv.messages[msg.channelId].forEach(function(m) {
          if (m.replyTo && m.replyTo.id == msg.messageId) {
            m.replyTo.deleted = true;
          }
        });
        if (state.currentServer === msg.serverId && state.currentChannel === msg.channelId) {
          renderMessages(srv.messages[msg.channelId]);
        }
      }
    },
    
    reaction_added: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && srv.messages[msg.channelId]) {
        var m = srv.messages[msg.channelId].find(function(x) { return x.id == msg.messageId; });
        if (m) {
          if (!m.reactions) m.reactions = {};
          if (!m.reactions[msg.emoji]) m.reactions[msg.emoji] = [];
          if (!m.reactions[msg.emoji].includes(msg.userId)) {
            m.reactions[msg.emoji].push(msg.userId);
          }
        }
        if (state.currentServer === msg.serverId && state.currentChannel === msg.channelId) {
          renderMessages(srv.messages[msg.channelId]);
        }
      }
    },
    
    reaction_removed: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && srv.messages[msg.channelId]) {
        var m = srv.messages[msg.channelId].find(function(x) { return x.id == msg.messageId; });
        if (m && m.reactions && m.reactions[msg.emoji]) {
          var idx = m.reactions[msg.emoji].indexOf(msg.userId);
          if (idx !== -1) m.reactions[msg.emoji].splice(idx, 1);
          if (m.reactions[msg.emoji].length === 0) delete m.reactions[msg.emoji];
        }
        if (state.currentServer === msg.serverId && state.currentChannel === msg.channelId) {
          renderMessages(srv.messages[msg.channelId]);
        }
      }
    },
    
    dm: function() {
      var senderId = msg.message.from;
      if (msg.sender) state.friends.set(senderId, msg.sender);
      if (!state.dmMessages.has(senderId)) state.dmMessages.set(senderId, []);
      state.dmMessages.get(senderId).push(msg.message);
      state.dmChats.add(senderId);
      renderDMList();
      if (state.currentDM === senderId) {
        appendDMMessage(msg.message);
      }
    },
    
    dm_sent: function() {
      var toId = msg.to;
      if (msg.recipient) state.friends.set(toId, msg.recipient);
      if (!state.dmMessages.has(toId)) state.dmMessages.set(toId, []);
      state.dmMessages.get(toId).push(msg.message);
      state.dmChats.add(toId);
      renderDMList();
      if (state.currentDM === toId) {
        removePendingMessages();
        appendDMMessage(msg.message);
      }
    },
    
    dm_error: function() {
      showNotification(msg.message || 'Ошибка отправки');
    },
    
    dm_history: function() {
      state.dmMessages.set(msg.oderId, msg.messages || []);
      if (state.currentDM === msg.oderId) {
        renderDMMessages();
      }
    },

    
    friend_request_sent: function() {
      showNotification('Заявка в друзья отправлена');
    },
    
    friend_error: function() {
      showNotification(msg.message || 'Ошибка');
    },
    
    friend_request_incoming: function() {
      state.pendingRequests.push(msg.user);
      renderFriends();
      showNotification(msg.user.name + ' хочет добавить вас в друзья');
    },
    
    friend_added: function() {
      state.friends.set(msg.user.id, msg.user);
      state.pendingRequests = state.pendingRequests.filter(function(r) { return r.id !== msg.user.id; });
      state.dmChats.add(msg.user.id);
      renderFriends();
      renderDMList();
      showNotification(msg.user.name + ' теперь ваш друг');
    },
    
    friend_removed: function() {
      state.friends.delete(msg.oderId);
      renderFriends();
    },
    
    friends_list: function() {
      state.friends.clear();
      msg.friends.forEach(function(f) {
        state.friends.set(f.id, f);
        state.dmChats.add(f.id);
      });
      state.pendingRequests = msg.requests || [];
      renderFriends();
      renderDMList();
    },
    
    user_blocked: function() {
      state.blockedUsers.add(msg.oderId);
      state.friends.delete(msg.oderId);
      renderFriends();
      showNotification('Пользователь заблокирован');
    },
    
    user_unblocked: function() {
      state.blockedUsers.delete(msg.oderId);
      showNotification('Пользователь разблокирован');
    },
    
    invite_created: function() {
      qS('#invite-code-display').value = msg.code;
      openModal('invite-modal');
    },
    
    invite_error: function() {
      qS('#invite-error').textContent = msg.message;
    },
    
    profile_updated: function() {
      state.username = msg.user.name;
      state.userAvatar = msg.user.avatar;
      state.userStatus = msg.user.status;
      state.customStatus = msg.user.customStatus;
      updateUserPanel();
    },
    
    settings_updated: function() {
      state.settings = msg.settings;
    },
    
    user_join: function() {
      if (msg.user) {
        state.friends.set(msg.user.id, msg.user);
        renderFriends();
        if (state.currentServer) {
          send({ type: 'get_server_members', serverId: state.currentServer });
        }
      }
    },
    
    user_leave: function() {
      var f = state.friends.get(msg.oderId);
      if (f) {
        f.status = 'offline';
        renderFriends();
        if (state.currentServer) {
          send({ type: 'get_server_members', serverId: state.currentServer });
        }
      }
    },
    
    user_update: function() {
      if (msg.user) {
        state.friends.set(msg.user.id, msg.user);
        renderFriends();
        if (state.currentServer) {
          send({ type: 'get_server_members', serverId: state.currentServer });
        }
      }
    },
    
    server_members: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) srv.membersData = msg.members;
      if (state.currentServer === msg.serverId) renderMembers();
      if (state.editingServerId === msg.serverId) renderServerMembersList();
    },
    
    member_joined: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && msg.user) {
        if (!srv.members.includes(msg.user.id)) srv.members.push(msg.user.id);
        if (state.currentServer === msg.serverId) {
          send({ type: 'get_server_members', serverId: msg.serverId });
        }
      }
    },
    
    member_left: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        srv.members = srv.members.filter(function(m) { return m !== msg.oderId; });
        if (state.currentServer === msg.serverId) {
          send({ type: 'get_server_members', serverId: msg.serverId });
        }
      }
    },
    
    member_banned: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        srv.members = srv.members.filter(function(m) { return m !== msg.oderId; });
        if (state.currentServer === msg.serverId) {
          send({ type: 'get_server_members', serverId: msg.serverId });
        }
      }
    },
    
    role_created: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        srv.roles.push(msg.role);
        if (state.editingServerId === msg.serverId) renderRoles();
      }
    },
    
    role_updated: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        var idx = srv.roles.findIndex(function(r) { return r.id === msg.role.id; });
        if (idx !== -1) srv.roles[idx] = msg.role;
        if (state.editingServerId === msg.serverId) renderRoles();
      }
    },
    
    role_deleted: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        srv.roles = srv.roles.filter(function(r) { return r.id !== msg.roleId; });
        if (state.editingServerId === msg.serverId) renderRoles();
      }
    },
    
    role_assigned: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        srv.memberRoles[msg.memberId] = msg.roleId;
        if (state.currentServer === msg.serverId) {
          send({ type: 'get_server_members', serverId: msg.serverId });
        }
      }
    },
    
    voice_state_update: function() {
      // Save voice users for this channel
      state.voiceUsers.set(msg.channelId, msg.users || []);
      
      // Update voice users display
      if (state.currentServer === msg.serverId) {
        renderVoiceUsers(msg.channelId, msg.users);
        renderChannels();
        
        // Initiate calls to new users in the channel
        // Only the user with "lower" ID initiates to avoid glare (both calling each other)
        if (state.voiceChannel === msg.channelId && msg.users) {
          msg.users.forEach(function(u) {
            if (u.id !== state.userId && !peerConnections.has(u.id)) {
              // Only initiate if our ID is "lower" (alphabetically)
              if (state.userId < u.id) {
                console.log('Initiating call to:', u.id, '(we are lower ID)');
                setTimeout(function() {
                  initiateCall(u.id);
                }, 500);
              } else {
                console.log('Waiting for call from:', u.id, '(they have lower ID)');
              }
            }
          });
        }
      }
    },
    
    voice_signal: function() {
      if (msg.from && msg.signal) {
        handleVoiceSignal(msg.from, msg.signal);
      }
    },
    
    search_results: function() {
      state.searchResults = msg.results;
      renderSearchResults();
    },
    
    user_search_results: function() {
      renderUserSearchResults(msg.results);
    }
  };
  
  if (handlers[msg.type]) handlers[msg.type]();
}


// ============ UI HELPERS ============
function showView(id) {
  qSA('.main-view').forEach(function(v) { v.classList.remove('active'); });
  var el = document.getElementById(id);
  if (el) el.classList.add('active');
}

function openModal(id) {
  var el = document.getElementById(id);
  if (el) el.classList.add('active');
}

function closeModal(id) {
  var el = document.getElementById(id);
  if (el) el.classList.remove('active');
}

function hideContextMenu() {
  qSA('.context-menu').forEach(function(m) { m.classList.remove('visible'); });
}

function showNotification(text) {
  var n = document.createElement('div');
  n.className = 'notification';
  n.textContent = text;
  document.body.appendChild(n);
  setTimeout(function() { n.classList.add('show'); }, 10);
  setTimeout(function() {
    n.classList.remove('show');
    setTimeout(function() { n.remove(); }, 300);
  }, 3000);
}

function updateUserPanel() {
  var av = qS('#user-avatar');
  var nm = qS('#user-name');
  var st = qS('#user-status');
  if (av) {
    if (state.userAvatar) {
      av.innerHTML = '<img src="' + state.userAvatar + '">';
    } else {
      av.innerHTML = '';
      av.textContent = state.username ? state.username.charAt(0).toUpperCase() : '?';
    }
  }
  if (nm) nm.textContent = state.username || 'Гость';
  if (st) st.textContent = state.customStatus || displayStatus(state.userStatus);
}

// ============ RENDER FUNCTIONS ============
function renderServers() {
  var c = qS('#servers-list');
  if (!c) return;
  
  var old = c.querySelectorAll('.server-btn:not(.home-btn):not(.add-server):not(.join-server)');
  old.forEach(function(el) { el.remove(); });
  
  var add = c.querySelector('.add-server');
  state.servers.forEach(function(srv) {
    var b = document.createElement('div');
    b.className = 'server-btn';
    b.dataset.id = srv.id;
    b.title = srv.name;
    if (srv.icon) {
      b.classList.add('has-icon');
      b.innerHTML = '<img src="' + srv.icon + '">';
    } else {
      b.textContent = srv.name.charAt(0).toUpperCase();
    }
    b.onclick = function() { openServer(srv.id); };
    b.oncontextmenu = function(e) {
      e.preventDefault();
      showServerContext(e.clientX, e.clientY, srv);
    };
    c.insertBefore(b, add);
  });
}

function renderChannels() {
  var srv = state.servers.get(state.currentServer);
  if (!srv) return;
  
  var tl = qS('#channel-list');
  var vl = qS('#voice-list');
  if (!tl || !vl) return;
  
  var th = '';
  (srv.channels || []).forEach(function(c) {
    th += '<div class="channel-item' + (state.currentChannel === c.id ? ' active' : '') + '" data-id="' + c.id + '">';
    th += '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 9h16M4 15h16M10 3L8 21M16 3l-2 18"/></svg>';
    th += '<span>' + escapeHtml(c.name) + '</span></div>';
  });
  tl.innerHTML = th;
  
  var vh = '';
  (srv.voiceChannels || []).forEach(function(vc) {
    var voiceUsers = state.voiceUsers.get(vc.id) || [];
    vh += '<div class="voice-channel-wrapper">';
    vh += '<div class="voice-item' + (state.voiceChannel === vc.id ? ' connected' : '') + '" data-id="' + vc.id + '">';
    vh += '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/></svg>';
    vh += '<span>' + escapeHtml(vc.name) + '</span>';
    if (vc.isTemporary) vh += '<span class="temp-badge">temp</span>';
    vh += '</div>';
    
    // Show users in voice channel
    if (voiceUsers.length > 0) {
      vh += '<div class="voice-channel-users">';
      voiceUsers.forEach(function(u) {
        vh += '<div class="voice-channel-user' + (u.muted ? ' muted' : '') + '">';
        vh += '<div class="voice-user-avatar">' + (u.avatar ? '<img src="' + u.avatar + '">' : (u.name ? u.name.charAt(0).toUpperCase() : '?')) + '</div>';
        vh += '<span class="voice-user-name">' + escapeHtml(u.name || 'User') + '</span>';
        if (u.muted) vh += '<svg class="mute-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/></svg>';
        vh += '</div>';
      });
      vh += '</div>';
    }
    vh += '</div>';
  });
  vl.innerHTML = vh;
  
  tl.querySelectorAll('.channel-item').forEach(function(el) {
    el.onclick = function() { openChannel(el.dataset.id); };
    el.oncontextmenu = function(e) {
      e.preventDefault();
      showChannelContext(e.clientX, e.clientY, el.dataset.id, false);
    };
  });
  
  vl.querySelectorAll('.voice-item').forEach(function(el) {
    el.onclick = function() { joinVoiceChannel(el.dataset.id); };
    el.oncontextmenu = function(e) {
      e.preventDefault();
      showChannelContext(e.clientX, e.clientY, el.dataset.id, true);
    };
  });
}


function renderMembers() {
  var srv = state.servers.get(state.currentServer);
  var ol = qS('#members-online');
  var ofl = qS('#members-offline');
  if (!srv || !ol || !ofl) return;
  
  var mems = srv.membersData || [];
  var on = mems.filter(function(m) { return m.status === 'online'; });
  var off = mems.filter(function(m) { return m.status !== 'online'; });
  
  qS('#online-count').textContent = on.length;
  qS('#offline-count').textContent = off.length;
  
  ol.innerHTML = on.map(memberHTML).join('');
  ofl.innerHTML = off.map(memberHTML).join('');
  
  // Bind member context menu
  qSA('.member-item').forEach(function(el) {
    el.oncontextmenu = function(e) {
      e.preventDefault();
      showMemberContext(e.clientX, e.clientY, el.dataset.id);
    };
  });
}

function memberHTML(m) {
  var crown = m.isOwner ? '<svg class="crown-icon" viewBox="0 0 24 24" fill="currentColor"><path d="M5 16L3 5l5.5 5L12 4l3.5 6L21 5l-2 11H5zm14 3c0 .6-.4 1-1 1H6c-.6 0-1-.4-1-1v-1h14v1z"/></svg>' : '';
  var role = m.role && m.role !== 'default' ? '<span class="role-badge">' + escapeHtml(m.role) + '</span>' : '';
  return '<div class="member-item" data-id="' + m.id + '">' +
    '<div class="avatar ' + (m.status || 'offline') + '">' + (m.avatar ? '<img src="' + m.avatar + '">' : (m.name ? m.name.charAt(0).toUpperCase() : '?')) + '</div>' +
    '<span>' + escapeHtml(m.name || 'User') + crown + role + '</span></div>';
}

function renderFriends() {
  var all = [];
  state.friends.forEach(function(f) { all.push(f); });
  var online = all.filter(function(f) { return f.status === 'online'; });
  
  var ol = qS('#online-users');
  var al = qS('#all-users');
  var pl = qS('#pending-users');
  var pc = qS('#pending-count');
  
  if (ol) ol.innerHTML = online.length ? online.map(userItemHTML).join('') : '<div class="empty">Нет друзей в сети</div>';
  if (al) al.innerHTML = all.length ? all.map(userItemHTML).join('') : '<div class="empty">Нет друзей</div>';
  
  if (pl) {
    pl.innerHTML = state.pendingRequests.length ? state.pendingRequests.map(pendingItemHTML).join('') : '<div class="empty">Нет запросов</div>';
    
    pl.querySelectorAll('.accept-btn').forEach(function(b) {
      b.onclick = function(e) {
        e.preventDefault();
        e.stopPropagation();
        var fromId = b.dataset.id;
        if (send({ type: 'friend_accept', from: fromId })) {
          showNotification('Принимаем запрос...');
        } else {
          showNotification('Ошибка соединения');
        }
      };
    });
    
    pl.querySelectorAll('.reject-btn').forEach(function(b) {
      b.onclick = function(e) {
        e.preventDefault();
        e.stopPropagation();
        send({ type: 'friend_reject', from: b.dataset.id });
        state.pendingRequests = state.pendingRequests.filter(function(r) { return r.id !== b.dataset.id; });
        renderFriends();
      };
    });
  }
  
  if (pc) pc.textContent = state.pendingRequests.length || '';
  
  qSA('.msg-btn').forEach(function(b) {
    b.onclick = function() { openDM(b.dataset.id); };
  });
}

function userItemHTML(u) {
  return '<div class="user-item" data-id="' + u.id + '">' +
    '<div class="avatar ' + (u.status || 'offline') + '">' + (u.avatar ? '<img src="' + u.avatar + '">' : (u.name ? u.name.charAt(0).toUpperCase() : '?')) + '</div>' +
    '<div class="info"><div class="name">' + escapeHtml(u.name || 'User') + '</div><div class="status">' + (u.customStatus || displayStatus(u.status)) + '</div></div>' +
    '<div class="actions"><button class="msg-btn" data-id="' + u.id + '"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg></button></div></div>';
}

function pendingItemHTML(u) {
  return '<div class="user-item" data-id="' + u.id + '">' +
    '<div class="avatar">' + (u.name ? u.name.charAt(0).toUpperCase() : '?') + '</div>' +
    '<div class="info"><div class="name">' + escapeHtml(u.name || 'User') + '</div><div class="status">Хочет добавить вас</div></div>' +
    '<div class="actions">' +
    '<button class="accept-btn" data-id="' + u.id + '"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg></button>' +
    '<button class="reject-btn" data-id="' + u.id + '"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>' +
    '</div></div>';
}

function renderDMList() {
  var dl = qS('#dm-list');
  if (!dl) return;
  
  var h = '';
  state.dmChats.forEach(function(oderId) {
    var f = state.friends.get(oderId);
    if (!f) {
      var msgs = state.dmMessages.get(oderId);
      if (msgs && msgs.length > 0) {
        var lastMsg = msgs[msgs.length - 1];
        f = { id: oderId, name: lastMsg.author || 'User', avatar: lastMsg.avatar, status: 'offline' };
      }
    }
    if (f && f.name) {
      h += '<div class="dm-item' + (state.currentDM === oderId ? ' active' : '') + '" data-id="' + oderId + '">' +
        '<div class="avatar ' + (f.status || 'offline') + '">' + (f.avatar ? '<img src="' + f.avatar + '">' : f.name.charAt(0).toUpperCase()) + '</div>' +
        '<span>' + escapeHtml(f.name) + '</span></div>';
    }
  });
  dl.innerHTML = h;
  
  dl.querySelectorAll('.dm-item').forEach(function(el) {
    el.onclick = function() { openDM(el.dataset.id); };
  });
}

function renderVoiceUsers(channelId, users) {
  var vu = qS('#voice-users');
  if (!vu || state.voiceChannel !== channelId) return;
  
  // Save screen share if exists
  var screenShare = qS('#local-screen-preview-container');
  var screenShareHTML = screenShare ? screenShare.outerHTML : '';
  var screenShareElement = screenShare;
  
  // Render users
  var usersHTML = (users || []).map(function(u) {
    return '<div class="voice-user" data-user-id="' + u.id + '">' +
      '<div class="avatar" data-user-id="' + u.id + '">' + (u.avatar ? '<img src="' + u.avatar + '">' : (u.name ? u.name.charAt(0).toUpperCase() : '?')) + '</div>' +
      '<span>' + escapeHtml(u.name) + '</span>' +
      (u.muted ? '<svg class="muted-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>' : '') +
      (u.video ? '<svg class="video-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>' : '') +
      (u.screen ? '<svg class="screen-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>' : '') +
      '</div>';
  }).join('');
  
  // Set HTML
  vu.innerHTML = usersHTML;
  
  // Re-add screen share at the beginning if it existed
  if (screenShareElement && screenShareElement.parentElement) {
    // Screen share still exists, do nothing
  } else if (screenShareHTML) {
    // Re-insert screen share
    vu.insertAdjacentHTML('afterbegin', screenShareHTML);
  }
  
  // Bind context menu to voice users
  qSA('.voice-user').forEach(function(el) {
    el.oncontextmenu = function(e) {
      e.preventDefault();
      var userId = el.dataset.userId;
      if (userId && userId !== state.userId) {
        showVoiceUserContext(e.clientX, e.clientY, userId);
      }
    };
  });
}

function renderSearchResults() {
  var sr = qS('#global-search-results');
  if (!sr) return;
  
  if (!state.searchResults || state.searchResults.length === 0) {
    sr.innerHTML = '<div class="empty">Ничего не найдено</div>';
    return;
  }
  
  var srv = state.servers.get(state.currentServer);
  sr.innerHTML = state.searchResults.map(function(r) {
    var ch = srv ? srv.channels.find(function(c) { return c.id === r.channelId; }) : null;
    return '<div class="search-result-item" data-channel="' + r.channelId + '" data-msg="' + r.id + '">' +
      '<div class="search-result-channel">#' + (ch ? escapeHtml(ch.name) : r.channelId) + ' • ' + formatTime(r.time) + '</div>' +
      '<div class="search-result-author">' + escapeHtml(r.author) + '</div>' +
      '<div class="search-result-text">' + escapeHtml(r.text) + '</div></div>';
  }).join('');
  
  sr.querySelectorAll('.search-result-item').forEach(function(item) {
    item.onclick = function() {
      openChannel(item.dataset.channel);
      closeModal('search-modal');
      setTimeout(function() {
        var msg = qS('.message[data-id="' + item.dataset.msg + '"]');
        if (msg) {
          msg.scrollIntoView({ behavior: 'smooth', block: 'center' });
          msg.classList.add('highlighted');
          setTimeout(function() { msg.classList.remove('highlighted'); }, 2000);
        }
      }, 100);
    };
  });
}

function renderRoles() {
  var rl = qS('#roles-list');
  if (!rl) return;
  
  var srv = state.servers.get(state.editingServerId);
  if (!srv || !srv.roles) {
    rl.innerHTML = '<div class="empty">Нет ролей</div>';
    return;
  }
  
  rl.innerHTML = srv.roles.map(function(role) {
    var isDefault = role.id === 'owner' || role.id === 'default';
    return '<div class="role-item" data-id="' + role.id + '">' +
      '<div class="role-info">' +
      '<div class="role-color" style="background: ' + (role.color || '#99aab5') + '"></div>' +
      '<span class="role-name">' + escapeHtml(role.name) + '</span>' +
      '</div>' +
      '<div class="role-actions">' +
      (isDefault ? '' : '<button class="btn secondary edit-role-btn" data-id="' + role.id + '">Изменить</button>') +
      (isDefault ? '' : '<button class="btn danger delete-role-btn" data-id="' + role.id + '">Удалить</button>') +
      '</div></div>';
  }).join('');
  
  rl.querySelectorAll('.edit-role-btn').forEach(function(btn) {
    btn.onclick = function(e) {
      e.stopPropagation();
      var role = srv.roles.find(function(r) { return r.id === btn.dataset.id; });
      if (role) {
        state.editingRoleId = role.id;
        qS('#role-modal-title').textContent = 'Редактировать роль';
        qS('#role-name-input').value = role.name;
        qS('#role-color-input').value = role.color || '#99aab5';
        qS('#role-color-preview').style.background = role.color || '#99aab5';
        setPermissionCheckboxes(role.permissions || []);
        openModal('role-modal');
      }
    };
  });
  
  rl.querySelectorAll('.delete-role-btn').forEach(function(btn) {
    btn.onclick = function(e) {
      e.stopPropagation();
      if (confirm('Удалить роль?')) {
        send({ type: 'delete_role', serverId: state.editingServerId, roleId: btn.dataset.id });
      }
    };
  });
}

function renderServerMembersList() {
  var ml = qS('#server-members-list');
  if (!ml) return;
  
  var srv = state.servers.get(state.editingServerId);
  if (!srv || !srv.membersData) {
    ml.innerHTML = '<div class="empty">Загрузка...</div>';
    return;
  }
  
  ml.innerHTML = srv.membersData.map(function(m) {
    var role = srv.roles ? srv.roles.find(function(r) { return r.id === (srv.memberRoles[m.id] || 'default'); }) : null;
    return '<div class="member-item clickable" data-id="' + m.id + '">' +
      '<div class="avatar ' + (m.status || 'offline') + '">' + (m.avatar ? '<img src="' + m.avatar + '">' : (m.name ? m.name.charAt(0).toUpperCase() : '?')) + '</div>' +
      '<div class="member-info">' +
      '<span class="member-name">' + escapeHtml(m.name) + (m.isOwner ? ' 👑' : '') + '</span>' +
      (role ? '<span class="role-badge" style="background: ' + (role.color || '#99aab5') + '22; color: ' + (role.color || '#99aab5') + '">' + escapeHtml(role.name) + '</span>' : '') +
      '</div></div>';
  }).join('');
  
  ml.querySelectorAll('.member-item').forEach(function(item) {
    item.onclick = function() {
      var memberId = item.dataset.id;
      var member = srv.membersData.find(function(m) { return m.id === memberId; });
      if (member && !member.isOwner && memberId !== state.userId) {
        openMemberModal(member, srv);
      }
    };
  });
}

function openMemberModal(member, srv) {
  state.editingMemberId = member.id;
  qS('#member-modal-name').textContent = member.name;
  var av = qS('#member-modal-avatar');
  if (av) {
    if (member.avatar) av.innerHTML = '<img src="' + member.avatar + '">';
    else av.textContent = member.name.charAt(0).toUpperCase();
  }
  
  var select = qS('#member-role-select');
  if (select && srv.roles) {
    select.innerHTML = srv.roles.filter(function(r) { return r.id !== 'owner'; }).map(function(r) {
      var selected = (srv.memberRoles[member.id] || 'default') === r.id ? ' selected' : '';
      return '<option value="' + r.id + '"' + selected + '>' + escapeHtml(r.name) + '</option>';
    }).join('');
  }
  
  openModal('member-modal');
}


function renderUserSearchResults(results) {
  var sr = qS('#search-results');
  if (!sr) return;
  
  if (!results || results.length === 0) {
    sr.innerHTML = '<div class="empty">Пользователь не найден</div>';
    return;
  }
  
  sr.innerHTML = results.map(function(u) {
    return '<div class="user-item search-result" data-id="' + u.id + '">' +
      '<div class="avatar">' + (u.avatar ? '<img src="' + u.avatar + '">' : (u.name ? u.name.charAt(0).toUpperCase() : '?')) + '</div>' +
      '<div class="info"><div class="name">' + escapeHtml(u.name) + '</div></div>' +
      '<div class="actions"><button class="add-friend-btn" data-name="' + escapeHtml(u.name) + '">Добавить</button></div></div>';
  }).join('');
  
  sr.querySelectorAll('.add-friend-btn').forEach(function(b) {
    b.onclick = function() {
      send({ type: 'friend_request', name: b.dataset.name });
    };
  });
}

// ============ MESSAGES ============
function messageHTML(m) {
  if (!m) return '';
  
  var t = formatTime(m.time || m.timestamp || m.createdAt || Date.now());
  var a = m.author || m.authorName || 'Удалённый пользователь';
  var txt = m.text || m.content || '';
  var pendingClass = m.pending ? ' pending' : '';
  var editedMark = m.edited ? '<span class="edited">(ред.)</span>' : '';
  var deletedUserClass = (!m.author && !m.authorName) ? ' deleted-user' : '';
  
  var replyHtml = '';
  if (m.replyTo) {
    if (m.replyTo.deleted) {
      replyHtml = '<div class="message-reply deleted"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg><span class="reply-content">Сообщение удалено</span></div>';
    } else {
      var ra = m.replyTo.author || '?';
      var rav = m.replyTo.avatar;
      replyHtml = '<div class="message-reply" data-reply-id="' + m.replyTo.id + '">' +
        '<div class="reply-avatar">' + (rav ? '<img src="' + rav + '">' : ra.charAt(0).toUpperCase()) + '</div>' +
        '<span class="reply-author">' + escapeHtml(ra) + '</span>' +
        '<span class="reply-content">' + escapeHtml((m.replyTo.text || '').substring(0, 50)) + '</span></div>';
    }
  }
  
  var forwardedHtml = '';
  if (m.forwarded) {
    forwardedHtml = '<div class="forwarded-info"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 14 20 9 15 4"/><path d="M4 20v-7a4 4 0 0 1 4-4h12"/></svg>Переслано от ' + escapeHtml(m.forwarded.from) + '</div>';
  }
  
  var reactionsHtml = '';
  if (m.reactions && Object.keys(m.reactions).length > 0) {
    reactionsHtml = '<div class="reactions">';
    Object.entries(m.reactions).forEach(function(entry) {
      var emoji = entry[0];
      var users = entry[1];
      var isMyReaction = users.includes(state.userId);
      reactionsHtml += '<button class="reaction' + (isMyReaction ? ' my-reaction' : '') + '" data-emoji="' + emoji + '" data-msg-id="' + m.id + '">' + emoji + ' ' + users.length + '</button>';
    });
    reactionsHtml += '</div>';
  }
  
  var attachmentsHtml = '';
  if (m.attachments && m.attachments.length > 0) {
    attachmentsHtml = '<div class="attachments">';
    m.attachments.forEach(function(att) {
      if (att.type === 'image') {
        attachmentsHtml += '<img src="' + att.url + '" class="attachment-image">';
      } else if (att.type === 'file') {
        attachmentsHtml += '<a href="' + att.url + '" class="attachment-file" download>' + escapeHtml(att.name) + '</a>';
      }
    });
    attachmentsHtml += '</div>';
  }
  
  return '<div class="message' + (m.replyTo ? ' has-reply' : '') + pendingClass + deletedUserClass + '" data-id="' + m.id + '" data-author-id="' + (m.oderId || m.authorId || '') + '" data-author="' + escapeHtml(a) + '" data-text="' + escapeHtml(txt) + '">' +
    replyHtml + forwardedHtml +
    '<div class="message-body">' +
    '<div class="avatar">' + (m.avatar ? '<img src="' + m.avatar + '">' : a.charAt(0).toUpperCase()) + '</div>' +
    '<div class="content">' +
    '<div class="header"><span class="author">' + escapeHtml(a) + '</span><span class="time">' + t + '</span>' + editedMark + '</div>' +
    '<div class="text">' + escapeHtml(txt) + '</div>' +
    attachmentsHtml +
    reactionsHtml +
    '</div></div></div>';
}

function renderMessages(msgs) {
  var c = qS('#messages');
  if (!c) return;
  c.innerHTML = (msgs || []).map(messageHTML).join('');
  c.scrollTop = c.scrollHeight;
  bindMessageEvents();
}

function appendMessage(m) {
  var c = qS('#messages');
  if (!c) return;
  c.insertAdjacentHTML('beforeend', messageHTML(m));
  c.scrollTop = c.scrollHeight;
  bindMessageEvents();
}

function renderDMMessages() {
  var c = qS('#dm-messages');
  if (!c) return;
  var msgs = state.dmMessages.get(state.currentDM) || [];
  c.innerHTML = msgs.map(messageHTML).join('');
  c.scrollTop = c.scrollHeight;
}

function appendDMMessage(m) {
  var c = qS('#dm-messages');
  if (!c) return;
  c.insertAdjacentHTML('beforeend', messageHTML(m));
  c.scrollTop = c.scrollHeight;
}

function removePendingMessages() {
  qSA('#dm-messages .message.pending').forEach(function(el) { el.remove(); });
}


function bindMessageEvents() {
  qSA('#messages .message').forEach(function(el) {
    el.oncontextmenu = function(e) {
      e.preventDefault();
      var isOwn = el.dataset.authorId === state.userId;
      showMessageContext(e.clientX, e.clientY, el.dataset.id, el.dataset.text, isOwn, el.dataset.author);
    };
  });
  
  qSA('#messages .message-reply').forEach(function(el) {
    el.onclick = function(e) {
      e.stopPropagation();
      var replyId = el.dataset.replyId;
      if (!replyId) return;
      var target = qS('.message[data-id="' + replyId + '"]');
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
        target.classList.add('highlighted');
        setTimeout(function() { target.classList.remove('highlighted'); }, 2000);
      }
    };
  });
  
  // Reaction buttons
  qSA('#messages .reaction').forEach(function(btn) {
    btn.onclick = function(e) {
      e.stopPropagation();
      var emoji = btn.dataset.emoji;
      var msgId = btn.dataset.msgId;
      var isMyReaction = btn.classList.contains('my-reaction');
      
      if (isMyReaction) {
        send({ type: 'remove_reaction', serverId: state.currentServer, channelId: state.currentChannel, messageId: msgId, emoji: emoji });
      } else {
        send({ type: 'add_reaction', serverId: state.currentServer, channelId: state.currentChannel, messageId: msgId, emoji: emoji });
      }
    };
  });
}

// ============ NAVIGATION ============
function openServer(id) {
  state.currentServer = id;
  state.currentDM = null;
  var srv = state.servers.get(id);
  
  qS('#server-name').textContent = srv ? srv.name : 'Сервер';
  
  qSA('.server-btn').forEach(function(b) {
    b.classList.toggle('active', b.dataset.id === id);
  });
  
  qSA('.sidebar-view').forEach(function(v) { v.classList.remove('active'); });
  qS('#server-view').classList.add('active');
  qS('#members-panel').classList.add('visible');
  
  renderChannels();
  send({ type: 'get_server_members', serverId: id });
  
  if (srv && srv.channels && srv.channels[0]) {
    openChannel(srv.channels[0].id);
  }
}

function openChannel(id) {
  state.currentChannel = id;
  var srv = state.servers.get(state.currentServer);
  var ch = srv ? srv.channels.find(function(c) { return c.id === id; }) : null;
  
  qS('#channel-name').textContent = ch ? ch.name : 'Канал';
  qS('#msg-input').placeholder = 'Написать в #' + (ch ? ch.name : 'канал');
  
  renderChannels();
  showView('chat-view');
  
  var msgs = srv && srv.messages ? srv.messages[id] : [];
  renderMessages(msgs || []);
}

function openDM(uid) {
  state.currentDM = uid;
  state.currentChannel = null;
  state.currentServer = null;
  state.dmChats.add(uid);
  
  var f = state.friends.get(uid);
  var n = f ? f.name : 'User';
  var av = f ? f.avatar : null;
  
  qS('#dm-header-name').textContent = n;
  var dha = qS('#dm-header-avatar');
  if (dha) {
    if (av) dha.innerHTML = '<img src="' + av + '">';
    else dha.textContent = n.charAt(0).toUpperCase();
  }
  qS('#dm-name').textContent = n;
  var da = qS('#dm-avatar');
  if (da) {
    if (av) da.innerHTML = '<img src="' + av + '">';
    else da.textContent = n.charAt(0).toUpperCase();
  }
  qS('#dm-input').placeholder = 'Написать @' + n;
  
  qSA('.server-btn').forEach(function(b) { b.classList.remove('active'); });
  qS('.home-btn').classList.add('active');
  
  qSA('.sidebar-view').forEach(function(v) { v.classList.remove('active'); });
  qS('#home-view').classList.add('active');
  qS('#members-panel').classList.remove('visible');
  
  showView('dm-view');
  renderDMList();
  
  send({ type: 'get_dm_history', oderId: uid });
  renderDMMessages();
}

// ============ WEBRTC VOICE ============
var peerConnections = new Map();
var localStream = null;
var audioAnalysers = new Map();
var audioContext = null;
var speakingCheckInterval = null;

var rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
    { urls: 'stun:stun3.l.google.com:19302' },
    { urls: 'stun:stun4.l.google.com:19302' }
  ],
  iceCandidatePoolSize: 10
};

function setupAudioAnalyser(stream, oderId) {
  if (!audioContext) {
    audioContext = new (window.AudioContext || window.webkitAudioContext)();
  }
  
  var source = audioContext.createMediaStreamSource(stream);
  var analyser = audioContext.createAnalyser();
  analyser.fftSize = 256;
  analyser.smoothingTimeConstant = 0.5;
  source.connect(analyser);
  
  audioAnalysers.set(oderId, analyser);
}

function checkSpeaking() {
  audioAnalysers.forEach(function(analyser, oderId) {
    var dataArray = new Uint8Array(analyser.frequencyBinCount);
    analyser.getByteFrequencyData(dataArray);
    
    var sum = 0;
    for (var i = 0; i < dataArray.length; i++) {
      sum += dataArray[i];
    }
    var average = sum / dataArray.length;
    
    // Try both selectors for compatibility
    var avatar = qS('.voice-user[data-user-id="' + oderId + '"] .avatar');
    if (!avatar) {
      avatar = qS('.voice-tile[data-user-id="' + oderId + '"] .avatar');
    }
    
    if (avatar) {
      if (average > 20) {
        avatar.classList.add('speaking');
      } else {
        avatar.classList.remove('speaking');
      }
    }
  });
}

function startSpeakingDetection() {
  if (speakingCheckInterval) return;
  speakingCheckInterval = setInterval(checkSpeaking, 100);
}

function stopSpeakingDetection() {
  if (speakingCheckInterval) {
    clearInterval(speakingCheckInterval);
    speakingCheckInterval = null;
  }
  audioAnalysers.clear();
}

function joinVoiceChannel(id) {
  if (state.voiceChannel === id) {
    // Don't leave, just show voice view
    showView('voice-view');
    return;
  }
  if (state.voiceChannel) leaveVoiceChannel();
  
  state.voiceChannel = id;
  
  // Get microphone access with noise suppression
  navigator.mediaDevices.getUserMedia({ 
    audio: {
      noiseSuppression: state.noiseSuppressionEnabled,
      echoCancellation: true,
      autoGainControl: true
    }, 
    video: false 
  })
    .then(function(stream) {
      localStream = stream;
      
      // Setup audio analyser for local user speaking detection
      setupAudioAnalyser(stream, state.userId);
      startSpeakingDetection();
      
      // Update noise button state
      var noiseBtn = qS('#voice-noise');
      if (noiseBtn) noiseBtn.classList.toggle('active', state.noiseSuppressionEnabled);
      
      send({ type: 'voice_join', channelId: id, serverId: state.currentServer });
      renderChannels();
      
      var srv = state.servers.get(state.currentServer);
      var ch = srv ? srv.voiceChannels.find(function(c) { return c.id === id; }) : null;
      qS('#voice-name').textContent = ch ? ch.name : 'Голосовой';
      showView('voice-view');
    })
    .catch(function(err) {
      console.error('Microphone error:', err);
      showNotification('Не удалось получить доступ к микрофону');
      state.voiceChannel = null;
    });
}

function leaveVoiceChannel() {
  // Stop speaking detection
  stopSpeakingDetection();
  
  // Remove all screen share windows
  document.querySelectorAll('.screen-share-window').forEach(function(el) {
    el.remove();
  });
  
  // Remove local preview
  var localPreview = document.getElementById('local-screen-preview-container');
  if (localPreview) {
    localPreview.remove();
  }
  
  // Stop screen sharing if active
  if (state.screenSharing) {
    if (state.screenStream) {
      state.screenStream.getTracks().forEach(function(track) { track.stop(); });
      state.screenStream = null;
    }
    state.screenSharing = false;
  }
  
  // Stop local stream
  if (localStream) {
    localStream.getTracks().forEach(function(track) { track.stop(); });
    localStream = null;
  }
  
  // Close all peer connections
  peerConnections.forEach(function(pc) {
    pc.close();
  });
  peerConnections.clear();
  
  send({ type: 'voice_leave', channelId: state.voiceChannel });
  state.voiceChannel = null;
  renderChannels();
  showView('chat-view');
}

function createPeerConnection(oderId) {
  if (peerConnections.has(oderId)) return peerConnections.get(oderId);
  
  var pc = new RTCPeerConnection(rtcConfig);
  peerConnections.set(oderId, pc);
  
  // Add local stream tracks (audio)
  if (localStream) {
    localStream.getTracks().forEach(function(track) {
      pc.addTrack(track, localStream);
    });
  }
  
  // Add screen share track if active
  if (state.screenStream) {
    state.screenStream.getTracks().forEach(function(track) {
      pc.addTrack(track, state.screenStream);
      console.log('Added screen track to new peer:', oderId);
    });
  }
  
  // Handle incoming tracks
  pc.ontrack = function(event) {
    console.log('Received remote track from:', oderId, 'kind:', event.track.kind);
    
    if (event.track.kind === 'audio') {
      // Remove existing audio element if any
      var existingAudio = document.getElementById('audio-' + oderId);
      if (existingAudio) existingAudio.remove();
      
      var audio = document.createElement('audio');
      audio.id = 'audio-' + oderId;
      audio.srcObject = event.streams[0];
      audio.autoplay = true;
      audio.playsInline = true;
      audio.volume = 1.0;
      document.body.appendChild(audio);
      
      // Force play with user interaction workaround
      var playPromise = audio.play();
      if (playPromise !== undefined) {
        playPromise.then(function() {
          console.log('Audio playing for:', oderId);
        }).catch(function(err) {
          console.error('Audio play error:', err);
          // Try to play on next user interaction
          document.addEventListener('click', function playOnClick() {
            audio.play();
            document.removeEventListener('click', playOnClick);
          }, { once: true });
        });
      }
      
      // Setup audio analyser for remote user speaking detection
      setupAudioAnalyser(event.streams[0], oderId);
    } else if (event.track.kind === 'video') {
      // Handle video track (screen share)
      console.log('Received video track from:', oderId);
      
      // Remove existing video element if any
      var existingContainer = document.getElementById('screen-share-container-' + oderId);
      if (existingContainer) existingContainer.remove();
      
      // Create container
      var container = document.createElement('div');
      container.id = 'screen-share-container-' + oderId;
      container.className = 'screen-share-window';
      container.style.position = 'fixed';
      container.style.bottom = '100px';
      container.style.right = '20px';
      container.style.width = '400px';
      container.style.height = '300px';
      container.style.zIndex = '1000';
      container.style.background = '#000';
      container.style.border = '2px solid var(--accent)';
      container.style.borderRadius = '8px';
      container.style.boxShadow = '0 8px 32px rgba(0,0,0,0.5)';
      container.style.display = 'flex';
      container.style.flexDirection = 'column';
      container.style.overflow = 'hidden';
      container.style.minWidth = '300px';
      container.style.minHeight = '200px';
      
      // Create header bar
      var header = document.createElement('div');
      header.style.background = 'var(--bg-secondary)';
      header.style.padding = '8px 12px';
      header.style.display = 'flex';
      header.style.alignItems = 'center';
      header.style.justifyContent = 'space-between';
      header.style.cursor = 'move';
      header.style.userSelect = 'none';
      
      var title = document.createElement('span');
      title.textContent = 'Демонстрация экрана';
      title.style.color = 'var(--text-primary)';
      title.style.fontSize = '14px';
      title.style.fontWeight = '500';
      
      var controls = document.createElement('div');
      controls.style.display = 'flex';
      controls.style.gap = '8px';
      
      // Fullscreen button
      var fullscreenBtn = document.createElement('button');
      fullscreenBtn.innerHTML = '⛶';
      fullscreenBtn.style.width = '24px';
      fullscreenBtn.style.height = '24px';
      fullscreenBtn.style.border = 'none';
      fullscreenBtn.style.borderRadius = '4px';
      fullscreenBtn.style.background = 'var(--bg-tertiary)';
      fullscreenBtn.style.color = 'var(--text-primary)';
      fullscreenBtn.style.fontSize = '16px';
      fullscreenBtn.style.cursor = 'pointer';
      fullscreenBtn.style.display = 'flex';
      fullscreenBtn.style.alignItems = 'center';
      fullscreenBtn.style.justifyContent = 'center';
      fullscreenBtn.title = 'Полный экран';
      
      // Close button
      var closeBtn = document.createElement('button');
      closeBtn.innerHTML = '✕';
      closeBtn.style.width = '24px';
      closeBtn.style.height = '24px';
      closeBtn.style.border = 'none';
      closeBtn.style.borderRadius = '4px';
      closeBtn.style.background = 'var(--danger)';
      closeBtn.style.color = 'white';
      closeBtn.style.fontSize = '16px';
      closeBtn.style.cursor = 'pointer';
      closeBtn.style.display = 'flex';
      closeBtn.style.alignItems = 'center';
      closeBtn.style.justifyContent = 'center';
      closeBtn.title = 'Закрыть';
      
      controls.appendChild(fullscreenBtn);
      controls.appendChild(closeBtn);
      header.appendChild(title);
      header.appendChild(controls);
      
      // Create video element
      var video = document.createElement('video');
      video.id = 'video-' + oderId;
      video.srcObject = event.streams[0];
      video.autoplay = true;
      video.playsInline = true;
      video.style.width = '100%';
      video.style.height = '100%';
      video.style.objectFit = 'contain';
      video.style.background = '#000';
      video.style.pointerEvents = 'none';
      video.style.flex = '1';
      
      // Create resize handle
      var resizeHandle = document.createElement('div');
      resizeHandle.style.position = 'absolute';
      resizeHandle.style.bottom = '0';
      resizeHandle.style.right = '0';
      resizeHandle.style.width = '30px';
      resizeHandle.style.height = '30px';
      resizeHandle.style.cursor = 'nwse-resize';
      resizeHandle.style.background = 'transparent';
      resizeHandle.style.zIndex = '10';
      resizeHandle.title = 'Изменить размер';
      
      // Add visual indicator
      var resizeIcon = document.createElement('div');
      resizeIcon.style.position = 'absolute';
      resizeIcon.style.bottom = '2px';
      resizeIcon.style.right = '2px';
      resizeIcon.style.width = '0';
      resizeIcon.style.height = '0';
      resizeIcon.style.borderStyle = 'solid';
      resizeIcon.style.borderWidth = '0 0 15px 15px';
      resizeIcon.style.borderColor = 'transparent transparent var(--accent) transparent';
      resizeIcon.style.pointerEvents = 'none';
      resizeHandle.appendChild(resizeIcon);
      
      container.appendChild(header);
      container.appendChild(video);
      container.appendChild(resizeHandle);
      document.body.appendChild(container);
      
      // Dragging and resizing state
      var dragState = {
        isDragging: false,
        isResizing: false,
        startX: 0,
        startY: 0,
        startLeft: 0,
        startTop: 0,
        startWidth: 0,
        startHeight: 0
      };
      
      // Mouse down on header - start dragging
      header.onmousedown = function(e) {
        // Don't drag if clicking buttons
        if (e.target === fullscreenBtn || e.target === closeBtn) {
          return;
        }
        
        dragState.isDragging = true;
        dragState.startX = e.clientX;
        dragState.startY = e.clientY;
        dragState.startLeft = container.offsetLeft;
        dragState.startTop = container.offsetTop;
        
        header.style.cursor = 'grabbing';
        e.preventDefault();
        e.stopPropagation();
        return false;
      };
      
      // Mouse down on resize handle - start resizing
      resizeHandle.onmousedown = function(e) {
        dragState.isResizing = true;
        dragState.startX = e.clientX;
        dragState.startY = e.clientY;
        dragState.startWidth = container.offsetWidth;
        dragState.startHeight = container.offsetHeight;
        
        e.preventDefault();
        e.stopPropagation();
        return false;
      };
      
      // Global mouse move
      var globalMouseMove = function(e) {
        if (dragState.isDragging) {
          var deltaX = e.clientX - dragState.startX;
          var deltaY = e.clientY - dragState.startY;
          
          container.style.left = (dragState.startLeft + deltaX) + 'px';
          container.style.top = (dragState.startTop + deltaY) + 'px';
          
          e.preventDefault();
          return false;
        }
        
        if (dragState.isResizing) {
          var deltaX = e.clientX - dragState.startX;
          var deltaY = e.clientY - dragState.startY;
          
          var newWidth = dragState.startWidth + deltaX;
          var newHeight = dragState.startHeight + deltaY;
          
          if (newWidth >= 400) {
            container.style.width = newWidth + 'px';
          }
          if (newHeight >= 300) {
            container.style.height = newHeight + 'px';
          }
          
          e.preventDefault();
          return false;
        }
      };
      
      // Global mouse up
      var globalMouseUp = function(e) {
        if (dragState.isDragging) {
          dragState.isDragging = false;
          header.style.cursor = 'move';
        }
        if (dragState.isResizing) {
          dragState.isResizing = false;
        }
      };
      
      // Add global listeners
      window.addEventListener('mousemove', globalMouseMove, true);
      window.addEventListener('mouseup', globalMouseUp, true);
      
      // Fullscreen toggle
      var isFullscreen = false;
      var savedStyle = {};
      fullscreenBtn.onclick = function() {
        if (!isFullscreen) {
          // Save current style
          savedStyle = {
            top: container.style.top,
            left: container.style.left,
            width: container.style.width,
            height: container.style.height,
            transform: container.style.transform
          };
          
          // Go fullscreen
          container.style.top = '0';
          container.style.left = '0';
          container.style.width = '100%';
          container.style.height = '100%';
          container.style.transform = 'none';
          container.style.borderRadius = '0';
          fullscreenBtn.innerHTML = '⛶';
          isFullscreen = true;
        } else {
          // Restore
          container.style.top = savedStyle.top;
          container.style.left = savedStyle.left;
          container.style.width = savedStyle.width;
          container.style.height = savedStyle.height;
          container.style.transform = savedStyle.transform;
          container.style.borderRadius = '8px';
          fullscreenBtn.innerHTML = '⛶';
          isFullscreen = false;
        }
      };
      
      closeBtn.onclick = function() {
        container.remove();
      };
      
      video.play().catch(function(err) {
        console.error('Video play error:', err);
      });
    }
  };
  
  // Handle ICE candidates
  pc.onicecandidate = function(event) {
    if (event.candidate) {
      send({
        type: 'voice_signal',
        to: oderId,
        signal: { type: 'candidate', candidate: event.candidate }
      });
    }
  };
  
  pc.onconnectionstatechange = function() {
    console.log('Connection state:', pc.connectionState, 'for user:', oderId);
    if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed') {
      removePeerConnection(oderId);
    }
  };
  
  return pc;
}

function removePeerConnection(oderId) {
  var pc = peerConnections.get(oderId);
  if (pc) {
    pc.close();
    peerConnections.delete(oderId);
  }
  var audio = document.getElementById('audio-' + oderId);
  if (audio) audio.remove();
  audioAnalysers.delete(oderId);
}

function handleVoiceSignal(fromId, signal) {
  console.log('Voice signal from:', fromId, 'type:', signal.type);
  var pc = peerConnections.get(fromId);
  
  if (signal.type === 'offer') {
    console.log('Received offer, creating answer...');
    pc = createPeerConnection(fromId);
    pc.setRemoteDescription(new RTCSessionDescription(signal))
      .then(function() {
        return pc.createAnswer();
      })
      .then(function(answer) {
        return pc.setLocalDescription(answer);
      })
      .then(function() {
        send({
          type: 'voice_signal',
          to: fromId,
          signal: pc.localDescription
        });
      })
      .catch(function(err) {
        console.error('Answer error:', err);
      });
  } else if (signal.type === 'answer') {
    if (pc) {
      pc.setRemoteDescription(new RTCSessionDescription(signal))
        .catch(function(err) {
          console.error('Set remote desc error:', err);
        });
    }
  } else if (signal.type === 'candidate' && signal.candidate) {
    if (pc) {
      pc.addIceCandidate(new RTCIceCandidate(signal.candidate))
        .catch(function(err) {
          console.error('Add ICE candidate error:', err);
        });
    }
  }
}

function initiateCall(oderId) {
  console.log('Initiating call to:', oderId);
  var pc = createPeerConnection(oderId);
  
  pc.createOffer()
    .then(function(offer) {
      console.log('Created offer for:', oderId);
      return pc.setLocalDescription(offer);
    })
    .then(function() {
      console.log('Sending offer to:', oderId);
      send({
        type: 'voice_signal',
        to: oderId,
        signal: pc.localDescription
      });
    })
    .catch(function(err) {
      console.error('Offer error:', err);
    });
}

function toggleMute() {
  if (localStream) {
    var audioTrack = localStream.getAudioTracks()[0];
    if (audioTrack) {
      audioTrack.enabled = !audioTrack.enabled;
      var muted = !audioTrack.enabled;
      send({ type: 'voice_mute', muted: muted });
      return muted;
    }
  }
  return false;
}

function toggleScreenShare() {
  if (state.screenSharing) {
    // Stop screen sharing
    if (state.screenStream) {
      state.screenStream.getTracks().forEach(function(track) { track.stop(); });
      state.screenStream = null;
    }
    
    // Remove local preview
    var localPreview = document.getElementById('local-screen-preview-container');
    if (localPreview) {
      localPreview.remove();
    }
    
    // Remove screen track from all peer connections
    peerConnections.forEach(function(pc) {
      var senders = pc.getSenders();
      senders.forEach(function(sender) {
        if (sender.track && sender.track.kind === 'video') {
          pc.removeTrack(sender);
        }
      });
    });
    
    state.screenSharing = false;
    var voiceScreenBtn = qS('#voice-screen');
    if (voiceScreenBtn) voiceScreenBtn.classList.remove('active');
    send({ type: 'voice_screen', screen: false });
    showNotification('Демонстрация экрана остановлена');
  } else {
    // Start screen sharing
    // Check if running in Electron
    if (window.electronAPI && window.electronAPI.getScreenSources) {
      // Electron screen share - show source picker
      window.electronAPI.getScreenSources().then(function(sources) {
        if (sources.length === 0) {
          showNotification('Нет доступных источников для демонстрации');
          return;
        }
        
        // Filter out MrDomestos window
        var filteredSources = sources.filter(function(s) {
          return !s.name.includes('MrDomestos') && !s.name.includes('mrdomestos');
        });
        
        if (filteredSources.length === 0) {
          filteredSources = sources;
        }
        
        // Show source picker dialog
        showScreenSourcePicker(filteredSources, function(selectedSource) {
          if (!selectedSource) return;
          
          // Get video stream
          navigator.mediaDevices.getUserMedia({
            audio: false,
            video: {
              mandatory: {
                chromeMediaSource: 'desktop',
                chromeMediaSourceId: selectedSource.id
              }
            }
          }).then(function(videoStream) {
            setupScreenShareStream(videoStream);
          }).catch(function(err) {
            console.error('Screen share error:', err);
            showNotification('Не удалось запустить демонстрацию экрана');
          });
        });
      }).catch(function(err) {
        console.error('Get sources error:', err);
        showNotification('Ошибка получения источников экрана');
      });
    } else if (navigator.mediaDevices && navigator.mediaDevices.getDisplayMedia) {
      // Browser screen share
      navigator.mediaDevices.getDisplayMedia({ 
        video: { 
          cursor: 'always'
        }, 
        audio: {
          echoCancellation: true,
          noiseSuppression: true
        }
      })
        .then(function(screenStream) {
          setupScreenShareStream(screenStream);
        })
        .catch(function(err) {
          console.error('Screen share error:', err);
          if (err.name !== 'NotAllowedError') {
            showNotification('Не удалось запустить демонстрацию экрана');
          }
        });
    } else {
      showNotification('Демонстрация экрана не поддерживается');
    }
  }
}

function showScreenSourcePicker(sources, callback) {
  // Create picker modal
  var overlay = document.createElement('div');
  overlay.className = 'screen-picker-overlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.8);z-index:10000;display:flex;align-items:center;justify-content:center;';
  
  var modal = document.createElement('div');
  modal.style.cssText = 'background:var(--bg-secondary);border-radius:12px;padding:24px;max-width:600px;max-height:80vh;overflow-y:auto;';
  
  var title = document.createElement('h2');
  title.textContent = 'Выберите источник для демонстрации';
  title.style.cssText = 'color:var(--text-primary);margin:0 0 20px 0;font-size:20px;';
  modal.appendChild(title);
  
  var grid = document.createElement('div');
  grid.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;';
  
  sources.forEach(function(source) {
    var item = document.createElement('div');
    item.style.cssText = 'background:var(--bg-tertiary);border-radius:8px;padding:12px;cursor:pointer;transition:all 0.2s;border:2px solid transparent;';
    item.onmouseenter = function() { item.style.borderColor = 'var(--accent)'; };
    item.onmouseleave = function() { item.style.borderColor = 'transparent'; };
    
    if (source.thumbnail) {
      var thumb = document.createElement('img');
      thumb.src = source.thumbnail.toDataURL();
      thumb.style.cssText = 'width:100%;height:80px;object-fit:cover;border-radius:4px;margin-bottom:8px;';
      item.appendChild(thumb);
    }
    
    var name = document.createElement('div');
    name.textContent = source.name.length > 20 ? source.name.substring(0, 20) + '...' : source.name;
    name.style.cssText = 'color:var(--text-primary);font-size:12px;text-align:center;';
    item.appendChild(name);
    
    item.onclick = function() {
      document.body.removeChild(overlay);
      callback(source);
    };
    
    grid.appendChild(item);
  });
  
  modal.appendChild(grid);
  
  var cancelBtn = document.createElement('button');
  cancelBtn.textContent = 'Отмена';
  cancelBtn.style.cssText = 'margin-top:20px;padding:10px 24px;background:var(--bg-tertiary);color:var(--text-primary);border:none;border-radius:6px;cursor:pointer;width:100%;';
  cancelBtn.onclick = function() {
    document.body.removeChild(overlay);
    callback(null);
  };
  modal.appendChild(cancelBtn);
  
  overlay.appendChild(modal);
  overlay.onclick = function(e) {
    if (e.target === overlay) {
      document.body.removeChild(overlay);
      callback(null);
    }
  };
  
  document.body.appendChild(overlay);
}

function setupScreenShareStream(screenStream) {
  state.screenSharing = true;
  state.screenStream = screenStream;
  
  var voiceScreenBtn = qS('#voice-screen');
  if (voiceScreenBtn) voiceScreenBtn.classList.add('active');
  
  // Show local preview
  showLocalScreenPreview(screenStream);
  
  // Add screen tracks to all peer connections
  var videoTrack = screenStream.getVideoTracks()[0];
  var audioTrack = screenStream.getAudioTracks()[0];
  
  peerConnections.forEach(function(pc, oderId) {
    if (videoTrack) {
      pc.addTrack(videoTrack, screenStream);
      console.log('Added screen video track to peer:', oderId);
    }
    if (audioTrack) {
      pc.addTrack(audioTrack, screenStream);
      console.log('Added screen audio track to peer:', oderId);
    }
    
    // Renegotiate connection
    pc.createOffer().then(function(offer) {
      return pc.setLocalDescription(offer);
    }).then(function() {
      send({
        type: 'voice_signal',
        to: oderId,
        signal: pc.localDescription
      });
    });
  });
  
  send({ type: 'voice_screen', screen: true });
  showNotification('Демонстрация экрана запущена');
  
  // Stop sharing when track ends
  if (videoTrack) {
    videoTrack.onended = function() {
      toggleScreenShare();
    };
  }
}

function showLocalScreenPreview(stream) {
  // Remove existing preview
  var existingContainer = document.getElementById('local-screen-preview-container');
  if (existingContainer) {
    existingContainer.remove();
  }
  
  // Add screen share to voice users view
  var voiceUsers = qS('#voice-users');
  if (!voiceUsers) return;
  
  // Create screen share container
  var screenDiv = document.createElement('div');
  screenDiv.id = 'local-screen-preview-container';
  screenDiv.className = 'voice-screen-share';
  screenDiv.style.position = 'relative';
  screenDiv.style.width = '600px';
  screenDiv.style.maxWidth = '90vw';
  screenDiv.style.background = '#000';
  screenDiv.style.borderRadius = '12px';
  screenDiv.style.overflow = 'hidden';
  screenDiv.style.border = '3px solid var(--accent)';
  screenDiv.style.boxShadow = '0 8px 32px rgba(168, 85, 247, 0.4)';
  
  // Create video element
  var video = document.createElement('video');
  video.id = 'local-screen-preview';
  video.srcObject = stream;
  video.autoplay = true;
  video.muted = true;
  video.playsInline = true;
  video.style.width = '100%';
  video.style.height = 'auto';
  video.style.display = 'block';
  video.style.background = '#000';
  
  // Create label overlay
  var label = document.createElement('div');
  label.textContent = 'Демонстрация экрана';
  label.style.position = 'absolute';
  label.style.bottom = '12px';
  label.style.left = '12px';
  label.style.background = 'rgba(0, 0, 0, 0.8)';
  label.style.color = 'white';
  label.style.padding = '6px 12px';
  label.style.borderRadius = '6px';
  label.style.fontSize = '14px';
  label.style.fontWeight = '500';
  label.style.backdropFilter = 'blur(10px)';
  
  screenDiv.appendChild(video);
  screenDiv.appendChild(label);
  
  // Fullscreen functionality
  var isFullscreen = false;
  var savedStyles = {};
  
  screenDiv.ondblclick = function() {
    if (!isFullscreen) {
      // Save current styles
      savedStyles = {
        position: screenDiv.style.position,
        width: screenDiv.style.width,
        maxWidth: screenDiv.style.maxWidth,
        borderRadius: screenDiv.style.borderRadius,
        zIndex: screenDiv.style.zIndex
      };
      
      // Go fullscreen
      screenDiv.style.position = 'fixed';
      screenDiv.style.top = '0';
      screenDiv.style.left = '0';
      screenDiv.style.width = '100vw';
      screenDiv.style.height = '100vh';
      screenDiv.style.maxWidth = '100vw';
      screenDiv.style.borderRadius = '0';
      screenDiv.style.zIndex = '9999';
      video.style.height = '100vh';
      video.style.objectFit = 'contain';
      label.textContent = 'Демонстрация экрана (двойной клик для выхода)';
      isFullscreen = true;
    } else {
      // Restore
      screenDiv.style.position = savedStyles.position;
      screenDiv.style.top = '';
      screenDiv.style.left = '';
      screenDiv.style.width = savedStyles.width;
      screenDiv.style.height = '';
      screenDiv.style.maxWidth = savedStyles.maxWidth;
      screenDiv.style.borderRadius = savedStyles.borderRadius;
      screenDiv.style.zIndex = savedStyles.zIndex || '';
      video.style.height = 'auto';
      video.style.objectFit = 'cover';
      label.textContent = 'Демонстрация экрана';
      isFullscreen = false;
    }
  };
  
  // Add cursor hint
  screenDiv.style.cursor = 'pointer';
  screenDiv.title = 'Двойной клик для полноэкранного режима';
  
  // Insert at the beginning of voice users
  if (voiceUsers.firstChild) {
    voiceUsers.insertBefore(screenDiv, voiceUsers.firstChild);
  } else {
    voiceUsers.appendChild(screenDiv);
  }
  
  // Wait a bit to ensure video is in DOM before playing
  setTimeout(function() {
    if (document.body.contains(video)) {
      video.play().catch(function(err) {
        if (document.body.contains(video)) {
          console.error('Preview play error:', err);
        }
      });
    }
  }, 100);
}

// Noise suppression
function applyNoiseSuppression(stream) {
  var audioTrack = stream.getAudioTracks()[0];
  if (audioTrack && audioTrack.applyConstraints) {
    audioTrack.applyConstraints({
      noiseSuppression: state.noiseSuppressionEnabled,
      echoCancellation: true,
      autoGainControl: true
    }).catch(function(err) {
      console.log('Noise suppression not supported:', err);
    });
  }
}

function loadAudioDevices() {
  navigator.mediaDevices.enumerateDevices().then(function(devices) {
    var inputSelect = qS('#voice-input-device');
    if (!inputSelect) return;
    
    inputSelect.innerHTML = '';
    devices.forEach(function(device) {
      if (device.kind === 'audioinput') {
        var option = document.createElement('option');
        option.value = device.deviceId;
        option.textContent = device.label || 'Микрофон ' + (inputSelect.options.length + 1);
        inputSelect.appendChild(option);
      }
    });
    
    // Add change handler
    inputSelect.onchange = function() {
      if (localStream) {
        // Restart stream with new device
        var constraints = {
          audio: { deviceId: inputSelect.value ? { exact: inputSelect.value } : undefined }
        };
        navigator.mediaDevices.getUserMedia(constraints).then(function(newStream) {
          localStream.getTracks().forEach(function(track) { track.stop(); });
          localStream = newStream;
          
          // Update peer connections with new stream
          peerConnections.forEach(function(pc) {
            var sender = pc.getSenders().find(function(s) { return s.track && s.track.kind === 'audio'; });
            if (sender) {
              sender.replaceTrack(newStream.getAudioTracks()[0]);
            }
          });
          
          setupAudioAnalyser(newStream, state.userId);
          showNotification('Устройство ввода изменено');
        }).catch(function(err) {
          console.error('Device change error:', err);
          showNotification('Ошибка смены устройства');
        });
      }
    };
  });
  
  // Start volume monitoring
  startVolumeMonitoring();
}

function startVolumeMonitoring() {
  var volumeBar = qS('#voice-volume-bar');
  if (!volumeBar) return;
  
  setInterval(function() {
    if (localStream && audioAnalysers.has(state.userId)) {
      var analyser = audioAnalysers.get(state.userId);
      var dataArray = new Uint8Array(analyser.frequencyBinCount);
      analyser.getByteFrequencyData(dataArray);
      
      var sum = 0;
      for (var i = 0; i < dataArray.length; i++) {
        sum += dataArray[i];
      }
      var average = sum / dataArray.length;
      var percent = Math.min(100, (average / 128) * 100);
      
      volumeBar.style.width = percent + '%';
    } else {
      volumeBar.style.width = '0%';
    }
  }, 50);
}

function testMicrophone() {
  navigator.mediaDevices.getUserMedia({ audio: true }).then(function(stream) {
    var audio = document.createElement('audio');
    audio.srcObject = stream;
    audio.autoplay = true;
    audio.volume = 1;
    document.body.appendChild(audio);
    
    showNotification('Проверка микрофона... Говорите');
    
    setTimeout(function() {
      stream.getTracks().forEach(function(t) { t.stop(); });
      audio.remove();
      showNotification('Проверка завершена');
    }, 5000);
  }).catch(function(err) {
    showNotification('Ошибка доступа к микрофону');
  });
}


// ============ CONTEXT MENUS ============
function showServerContext(x, y, srv) {
  var ctx = qS('#server-context');
  if (!ctx) return;
  ctx.style.left = x + 'px';
  ctx.style.top = y + 'px';
  ctx.classList.add('visible');
  ctx.dataset.serverId = srv.id;
}

function showChannelContext(x, y, channelId, isVoice) {
  var ctx = qS('#channel-context');
  if (!ctx) return;
  ctx.style.left = x + 'px';
  ctx.style.top = y + 'px';
  ctx.classList.add('visible');
  ctx.dataset.channelId = channelId;
  ctx.dataset.isVoice = isVoice ? '1' : '0';
}

function showMessageContext(x, y, msgId, msgText, isOwn, msgAuthor) {
  var ctx = qS('#message-context');
  if (!ctx) return;
  ctx.style.left = x + 'px';
  ctx.style.top = y + 'px';
  ctx.classList.add('visible');
  ctx.dataset.msgId = msgId;
  ctx.dataset.msgText = msgText;
  ctx.dataset.msgAuthor = msgAuthor || '';
  ctx.dataset.isOwn = isOwn ? '1' : '0';
  var delBtn = ctx.querySelector('[data-action="delete-message"]');
  if (delBtn) delBtn.style.display = isOwn ? 'flex' : 'none';
}

function positionContextMenu(ctx, x, y) {
  // Get menu dimensions
  ctx.style.visibility = 'hidden';
  ctx.style.display = 'block';
  var menuWidth = ctx.offsetWidth;
  var menuHeight = ctx.offsetHeight;
  ctx.style.display = '';
  ctx.style.visibility = '';
  
  // Get viewport dimensions
  var viewWidth = window.innerWidth;
  var viewHeight = window.innerHeight;
  
  // Adjust position if menu would go off screen
  if (x + menuWidth > viewWidth - 10) {
    x = viewWidth - menuWidth - 10;
  }
  if (y + menuHeight > viewHeight - 10) {
    y = viewHeight - menuHeight - 10;
  }
  if (x < 10) x = 10;
  if (y < 10) y = 10;
  
  ctx.style.left = x + 'px';
  ctx.style.top = y + 'px';
}

function showMemberContext(x, y, memberId) {
  hideContextMenu();
  var ctx = qS('#member-context');
  if (!ctx) return;
  
  // Don't show menu for yourself
  if (memberId === state.userId) return;
  
  var srv = state.servers.get(state.currentServer);
  var isOwner = srv && srv.ownerId === state.userId;
  var isMemberOwner = srv && srv.ownerId === memberId;
  
  // Show/hide admin buttons
  var kickBtn = ctx.querySelector('[data-action="kick-member"]');
  var banBtn = ctx.querySelector('[data-action="ban-member"]');
  var divider = ctx.querySelector('.divider');
  var showAdmin = isOwner && !isMemberOwner;
  if (kickBtn) kickBtn.style.display = showAdmin ? 'flex' : 'none';
  if (banBtn) banBtn.style.display = showAdmin ? 'flex' : 'none';
  if (divider) divider.style.display = showAdmin ? 'block' : 'none';
  
  positionContextMenu(ctx, x, y);
  ctx.classList.add('visible');
  ctx.dataset.userId = memberId;
  
  // Bind actions
  var viewProfileBtn = ctx.querySelector('[data-action="view-profile"]');
  if (viewProfileBtn) {
    viewProfileBtn.onclick = function() {
      hideContextMenu();
      showUserProfile(memberId);
    };
  }
  
  ctx.querySelector('[data-action="send-dm"]').onclick = function() {
    hideContextMenu();
    openDM(memberId);
  };
  
  var voiceCallBtn = ctx.querySelector('[data-action="voice-call"]');
  if (voiceCallBtn) {
    voiceCallBtn.onclick = function() {
      hideContextMenu();
      startPrivateCall(memberId);
    };
  }
  
  ctx.querySelector('[data-action="add-friend"]').onclick = function() {
    hideContextMenu();
    send({ type: 'friend_request', to: memberId });
    showNotification('Запрос в друзья отправлен!');
  };
  
  if (kickBtn) {
    kickBtn.onclick = function() {
      hideContextMenu();
      if (confirm('Исключить пользователя с сервера?')) {
        send({ type: 'kick_member', serverId: state.currentServer, memberId: memberId });
      }
    };
  }
  
  if (banBtn) {
    banBtn.onclick = function() {
      hideContextMenu();
      if (confirm('Забанить пользователя на сервере?')) {
        send({ type: 'ban_member', serverId: state.currentServer, memberId: memberId });
      }
    };
  }
}

function showUserProfile(userId) {
  var modal = qS('#user-profile-modal');
  if (!modal) return;
  
  // Get user data
  var user = null;
  var srv = state.servers.get(state.currentServer);
  if (srv && srv.membersData) {
    user = srv.membersData.find(function(m) { return m.id === userId; });
  }
  if (!user) {
    user = state.friends.get(userId);
  }
  if (!user) {
    user = { id: userId, name: 'Пользователь', status: 'offline' };
  }
  
  // Set avatar
  var avatarEl = qS('#profile-avatar');
  if (avatarEl) {
    if (user.avatar) {
      avatarEl.innerHTML = '<img src="' + user.avatar + '">';
    } else {
      avatarEl.textContent = user.name ? user.name.charAt(0).toUpperCase() : '?';
    }
  }
  
  // Set status badge
  var statusBadge = qS('#profile-status-badge');
  if (statusBadge) {
    statusBadge.className = 'profile-status-badge ' + (user.status || 'offline');
  }
  
  // Set name
  var nameEl = qS('#profile-name');
  if (nameEl) nameEl.textContent = user.name || 'Пользователь';
  
  // Set custom status
  var customStatusEl = qS('#profile-custom-status');
  if (customStatusEl) {
    customStatusEl.textContent = user.customStatus || '';
    customStatusEl.style.display = user.customStatus ? 'block' : 'none';
  }
  
  // Set status text
  var statusTextEl = qS('#profile-status-text');
  if (statusTextEl) {
    var statusMap = { online: 'В сети', offline: 'Не в сети', idle: 'Неактивен', dnd: 'Не беспокоить' };
    statusTextEl.textContent = statusMap[user.status] || 'Не в сети';
  }
  
  // Set created date
  var createdEl = qS('#profile-created');
  if (createdEl) {
    createdEl.textContent = user.createdAt ? new Date(user.createdAt).toLocaleDateString('ru-RU') : '—';
  }
  
  // Set mutual servers
  var mutualSection = qS('#profile-mutual-section');
  var mutualServers = qS('#profile-mutual-servers');
  if (mutualServers && mutualSection) {
    var mutuals = [];
    state.servers.forEach(function(s) {
      if (s.members && (s.members.has ? s.members.has(userId) : s.members.includes(userId))) {
        mutuals.push(s);
      }
    });
    
    if (mutuals.length > 0) {
      mutualSection.style.display = 'block';
      mutualServers.innerHTML = mutuals.map(function(s) {
        return '<div class="mutual-server"><div class="server-icon">' + (s.name ? s.name.charAt(0).toUpperCase() : '?') + '</div>' + escapeHtml(s.name) + '</div>';
      }).join('');
    } else {
      mutualSection.style.display = 'none';
    }
  }
  
  // Bind action buttons
  var dmBtn = qS('#profile-dm-btn');
  if (dmBtn) {
    dmBtn.onclick = function() {
      closeModal('user-profile-modal');
      openDM(userId);
    };
  }
  
  var callBtn = qS('#profile-call-btn');
  if (callBtn) {
    callBtn.onclick = function() {
      closeModal('user-profile-modal');
      startPrivateCall(userId);
    };
  }
  
  var friendBtn = qS('#profile-friend-btn');
  if (friendBtn) {
    friendBtn.onclick = function() {
      send({ type: 'friend_request', to: userId });
      showNotification('Запрос в друзья отправлен!');
    };
  }
  
  modal.classList.add('visible');
}

function startPrivateCall(userId) {
  // Open DM and start voice call
  openDM(userId);
  showNotification('Звонок пока не реализован в ЛС');
}

function showVoiceUserContext(x, y, userId) {
  hideContextMenu();
  var ctx = qS('#voice-user-context');
  if (!ctx) return;
  
  positionContextMenu(ctx, x, y);
  ctx.classList.add('visible');
  ctx.dataset.userId = userId;
  
  // Bind actions
  ctx.querySelector('[data-action="send-dm"]').onclick = function() {
    hideContextMenu();
    openDM(userId);
  };
  
  ctx.querySelector('[data-action="add-friend"]').onclick = function() {
    hideContextMenu();
    send({ type: 'friend_request', to: userId });
    showNotification('Запрос в друзья отправлен!');
  };
}

function showReplyBar() {
  var bar = qS('#reply-bar');
  if (!bar || !state.replyingTo) return;
  bar.querySelector('.reply-name').textContent = state.replyingTo.author;
  bar.querySelector('.reply-text').textContent = state.replyingTo.text.substring(0, 50) + (state.replyingTo.text.length > 50 ? '...' : '');
  bar.classList.add('visible');
  qS('#msg-input').focus();
}

function hideReplyBar() {
  var bar = qS('#reply-bar');
  if (bar) bar.classList.remove('visible');
  state.replyingTo = null;
}

// ============ AUDIO ============
// ============ SIGN OUT ============
function signOut() {
  localStorage.removeItem('session');
  localStorage.removeItem('lastEmail');
  localStorage.removeItem('lastPwd');
  
  state.userId = null;
  state.username = null;
  state.userAvatar = null;
  state.servers.clear();
  state.friends.clear();
  state.pendingRequests = [];
  state.currentServer = null;
  state.currentChannel = null;
  state.currentDM = null;
  state.dmMessages.clear();
  state.dmChats.clear();
  
  qS('#main-app').classList.add('hidden');
  qS('#auth-screen').classList.add('active');
  qS('#login-box').classList.remove('hidden');
  qS('#register-box').classList.add('hidden');
  
  closeModal('settings-modal');
}


// ============ EVENT LISTENERS ============
document.addEventListener('DOMContentLoaded', function() {
  // Window controls (Electron)
  if (window.electronAPI) {
    var minBtn = qS('#minimize-btn');
    var maxBtn = qS('#maximize-btn');
    var closeBtn = qS('#close-btn');
    
    if (minBtn) minBtn.onclick = function() { window.electronAPI.minimize(); };
    if (maxBtn) maxBtn.onclick = function() { window.electronAPI.maximize(); };
    if (closeBtn) closeBtn.onclick = function() { window.electronAPI.close(); };
  }
  
  // Auth
  qS('#login-btn').onclick = function() {
    var email = qS('#login-email').value.trim();
    var pwd = qS('#login-pass').value;
    if (!email || !pwd) return;
    localStorage.setItem('lastEmail', email);
    localStorage.setItem('lastPwd', pwd);
    showAuthLoading('Входим в аккаунт...');
    send({ type: 'login', email: email, password: pwd });
  };
  
  qS('#reg-btn').onclick = function() {
    var name = qS('#reg-name').value.trim();
    var email = qS('#reg-email').value.trim();
    var pwd = qS('#reg-pass').value;
    if (!name || !email || !pwd) return;
    localStorage.setItem('lastEmail', email);
    localStorage.setItem('lastPwd', pwd);
    showAuthLoading('Создаём аккаунт...');
    send({ type: 'register', email: email, password: pwd, name: name });
  };
  
  var guestBtn = qS('#guest-btn');
  if (guestBtn) {
    guestBtn.onclick = function() {
      showAuthLoading('Входим как гость...');
      send({ type: 'guest_login' });
    };
  }
  
  qS('#show-register').onclick = function(e) {
    e.preventDefault();
    qS('#login-box').classList.add('hidden');
    qS('#register-box').classList.remove('hidden');
  };
  
  qS('#show-login').onclick = function(e) {
    e.preventDefault();
    qS('#register-box').classList.add('hidden');
    qS('#login-box').classList.remove('hidden');
  };
  
  // Show/hide password
  var showLoginPass = qS('#show-login-pass');
  if (showLoginPass) {
    showLoginPass.onclick = function() {
      var inp = qS('#login-pass');
      if (inp.type === 'password') {
        inp.type = 'text';
        showLoginPass.textContent = 'Скрыть';
      } else {
        inp.type = 'password';
        showLoginPass.textContent = 'Показать';
      }
    };
  }
  
  var showRegPass = qS('#show-reg-pass');
  if (showRegPass) {
    showRegPass.onclick = function() {
      var inp = qS('#reg-pass');
      if (inp.type === 'password') {
        inp.type = 'text';
        showRegPass.textContent = 'Скрыть';
      } else {
        inp.type = 'password';
        showRegPass.textContent = 'Показать';
      }
    };
  }
  
  // Home button
  qS('.home-btn').onclick = function() {
    state.currentServer = null;
    state.currentChannel = null;
    qSA('.server-btn').forEach(function(b) { b.classList.remove('active'); });
    qS('.home-btn').classList.add('active');
    qSA('.sidebar-view').forEach(function(v) { v.classList.remove('active'); });
    qS('#home-view').classList.add('active');
    qS('#members-panel').classList.remove('visible');
    showView('friends-view');
  };
  
  // Tabs
  qSA('.tab').forEach(function(tab) {
    tab.onclick = function() {
      qSA('.tab').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('.tab-content').forEach(function(c) { c.classList.remove('active'); });
      var content = qS('#tab-' + tab.dataset.tab);
      if (content) content.classList.add('active');
    };
  });
  
  // Send message
  qS('#msg-input').onkeypress = function(e) {
    if (e.key === 'Enter') sendMessage();
  };
  qS('#send-btn').onclick = sendMessage;
  
  function sendMessage() {
    var input = qS('#msg-input');
    var text = input.value.trim();
    if (!text || !state.currentServer || !state.currentChannel) return;
    
    var data = { type: 'message', serverId: state.currentServer, channel: state.currentChannel, text: text };
    if (state.replyingTo) {
      data.replyTo = { id: state.replyingTo.id, author: state.replyingTo.author, text: state.replyingTo.text, avatar: state.replyingTo.avatar };
    }
    send(data);
    input.value = '';
    hideReplyBar();
  }
  
  // Send DM
  qS('#dm-input').onkeypress = function(e) {
    if (e.key === 'Enter') sendDM();
  };
  qS('#dm-send-btn').onclick = sendDM;
  
  function sendDM() {
    var input = qS('#dm-input');
    var text = input.value.trim();
    if (!text || !state.currentDM) return;
    
    var tempMsg = {
      id: 'temp_' + Date.now(),
      from: state.userId,
      to: state.currentDM,
      author: state.username,
      avatar: state.userAvatar,
      text: text,
      time: Date.now(),
      pending: true
    };
    appendDMMessage(tempMsg);
    
    send({ type: 'dm', to: state.currentDM, text: text });
    input.value = '';
  }

  
  // Create server
  qS('#add-server-btn').onclick = function() { openModal('create-server-modal'); };
  qS('#create-server-btn').onclick = function() {
    var name = qS('#new-server-name').value.trim();
    if (!name) return;
    send({ type: 'create_server', name: name, icon: state.newServerIcon });
    qS('#new-server-name').value = '';
    state.newServerIcon = null;
  };
  
  // Join server
  var joinServerBtn = qS('#join-server-btn');
  if (joinServerBtn) {
    joinServerBtn.onclick = function() { openModal('join-modal'); };
  }
  var useInviteBtn = qS('#use-invite-btn');
  if (useInviteBtn) {
    useInviteBtn.onclick = function() {
      var code = qS('#invite-code').value.trim();
      if (!code) return;
      send({ type: 'use_invite', code: code });
    };
  }
  
  var inviteCodeInput = qS('#invite-code');
  if (inviteCodeInput) {
    inviteCodeInput.onkeypress = function(e) {
      if (e.key === 'Enter') {
        var code = inviteCodeInput.value.trim();
        if (code) send({ type: 'use_invite', code: code });
      }
    };
  }
  
  // Create channel
  qS('#add-channel-btn').onclick = function() {
    state.creatingVoice = false;
    openModal('channel-modal');
  };
  qS('#add-voice-btn').onclick = function() {
    state.creatingVoice = true;
    openModal('channel-modal');
  };
  qS('#create-channel-btn').onclick = function() {
    var name = qS('#new-channel-name').value.trim();
    if (!name || !state.currentServer) return;
    send({ type: 'create_channel', serverId: state.currentServer, name: name, isVoice: state.creatingVoice });
    qS('#new-channel-name').value = '';
  };
  
  // Friend request
  qS('#search-btn').onclick = function() {
    var name = qS('#search-input').value.trim();
    if (!name) return;
    send({ type: 'friend_request', name: name });
    qS('#search-input').value = '';
  };
  
  // Settings
  qS('#settings-btn').onclick = function() {
    openModal('settings-modal');
    qS('#settings-name').value = state.username || '';
    var av = qS('#settings-avatar');
    if (av) {
      if (state.userAvatar) av.innerHTML = '<img src="' + state.userAvatar + '">';
      else av.textContent = state.username ? state.username.charAt(0).toUpperCase() : '?';
    }
  };
  
  qS('#save-profile').onclick = function() {
    var name = qS('#settings-name').value.trim();
    if (name) send({ type: 'update_profile', name: name });
  };
  
  qS('#signout-btn').onclick = signOut;
  
  // Settings tabs
  qSA('.settings-tab').forEach(function(tab) {
    tab.onclick = function() {
      var settingsType = tab.dataset.settings;
      if (!settingsType) return;
      qSA('.settings-tab[data-settings]').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('#settings-modal .settings-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#settings-' + settingsType);
      if (panel) panel.classList.add('active');
    };
  });
  
  // Close modals
  qSA('[data-close]').forEach(function(btn) {
    btn.onclick = function() {
      var modal = btn.closest('.modal');
      if (modal) modal.classList.remove('active');
    };
  });
  
  // Hide context menu on click
  document.onclick = function() { hideContextMenu(); };
  
  // Reply close
  qS('#reply-close').onclick = hideReplyBar;
  
  // Server context menu
  var serverCtx = qS('#server-context');
  if (serverCtx) {
    serverCtx.querySelector('[data-action="invite"]').onclick = function() {
      send({ type: 'create_invite', serverId: serverCtx.dataset.serverId });
    };
    serverCtx.querySelector('[data-action="settings"]').onclick = function() {
      state.editingServerId = serverCtx.dataset.serverId;
      var srv = state.servers.get(state.editingServerId);
      if (srv) {
        qS('#edit-server-name').value = srv.name;
        var icon = qS('#edit-server-icon');
        if (srv.icon) icon.innerHTML = '<img src="' + srv.icon + '">';
        else icon.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>';
      }
      openModal('server-settings-modal');
      send({ type: 'get_server_members', serverId: state.editingServerId });
      renderRoles();
      setTimeout(renderServerMembersList, 500);
    };
    serverCtx.querySelector('[data-action="leave"]').onclick = function() {
      if (confirm('Покинуть сервер?')) {
        send({ type: 'leave_server', serverId: serverCtx.dataset.serverId });
      }
    };
  }

  
  // Channel context menu
  var channelCtx = qS('#channel-context');
  if (channelCtx) {
    var editChBtn = channelCtx.querySelector('[data-action="edit-channel"]');
    if (editChBtn) {
      editChBtn.onclick = function() {
        state.editingChannelId = channelCtx.dataset.channelId;
        var srv = state.servers.get(state.currentServer);
        if (srv) {
          var isVoice = channelCtx.dataset.isVoice === '1';
          var channels = isVoice ? srv.voiceChannels : srv.channels;
          var ch = channels.find(function(c) { return c.id === state.editingChannelId; });
          if (ch) {
            qS('#edit-channel-name').value = ch.name;
            openModal('edit-channel-modal');
          }
        }
      };
    }
    
    var delChBtn = channelCtx.querySelector('[data-action="delete-channel"]');
    if (delChBtn) {
      delChBtn.onclick = function() {
        var srv = state.servers.get(state.currentServer);
        if (srv) {
          var isVoice = channelCtx.dataset.isVoice === '1';
          var channels = isVoice ? srv.voiceChannels : srv.channels;
          var ch = channels.find(function(c) { return c.id === channelCtx.dataset.channelId; });
          if (ch) {
            qS('#delete-channel-name').textContent = ch.name;
            state.editingChannelId = channelCtx.dataset.channelId;
            openModal('confirm-delete-channel-modal');
          }
        }
      };
    }
  }
  
  // Confirm delete channel
  var confirmDelChBtn = qS('#confirm-delete-channel-btn');
  if (confirmDelChBtn) {
    confirmDelChBtn.onclick = function() {
      var channelCtx = qS('#channel-context');
      send({
        type: 'delete_channel',
        serverId: state.currentServer,
        channelId: state.editingChannelId,
        isVoice: channelCtx?.dataset.isVoice === '1'
      });
      closeModal('confirm-delete-channel-modal');
    };
  }
  
  // Save channel settings
  var saveChBtn = qS('#save-channel-settings');
  if (saveChBtn) {
    saveChBtn.onclick = function() {
      var name = qS('#edit-channel-name').value.trim();
      if (name && state.editingChannelId) {
        var channelCtx = qS('#channel-context');
        send({
          type: 'update_channel',
          serverId: state.currentServer,
          channelId: state.editingChannelId,
          name: name,
          isVoice: channelCtx?.dataset.isVoice === '1'
        });
        closeModal('edit-channel-modal');
      }
    };
  }
  
  // Message context menu
  var msgCtx = qS('#message-context');
  if (msgCtx) {
    msgCtx.querySelector('[data-action="reply"]').onclick = function() {
      state.replyingTo = {
        id: msgCtx.dataset.msgId,
        author: msgCtx.dataset.msgAuthor,
        text: msgCtx.dataset.msgText
      };
      showReplyBar();
    };
    msgCtx.querySelector('[data-action="copy-text"]').onclick = function() {
      navigator.clipboard.writeText(msgCtx.dataset.msgText);
      showNotification('Текст скопирован');
    };
    msgCtx.querySelector('[data-action="delete-message"]').onclick = function() {
      send({
        type: 'delete_message',
        serverId: state.currentServer,
        channelId: state.currentChannel,
        messageId: msgCtx.dataset.msgId
      });
    };
    
    var forwardBtn = msgCtx.querySelector('[data-action="forward"]');
    if (forwardBtn) {
      forwardBtn.onclick = function() {
        state.forwardingMessage = {
          id: msgCtx.dataset.msgId,
          text: msgCtx.dataset.msgText,
          author: msgCtx.dataset.msgAuthor
        };
        openForwardModal();
      };
    }
  }
  
  function openForwardModal() {
    var list = qS('#forward-list');
    var preview = qS('#forward-msg-preview');
    if (!list || !state.forwardingMessage) return;
    
    if (preview) {
      preview.textContent = state.forwardingMessage.text.substring(0, 100);
    }
    
    var h = '';
    // Add DM chats
    state.friends.forEach(function(f) {
      h += '<div class="forward-item" data-type="dm" data-id="' + f.id + '">' +
        '<div class="avatar">' + (f.avatar ? '<img src="' + f.avatar + '">' : f.name.charAt(0).toUpperCase()) + '</div>' +
        '<span>' + escapeHtml(f.name) + '</span></div>';
    });
    // Add server channels
    state.servers.forEach(function(srv) {
      srv.channels.forEach(function(ch) {
        h += '<div class="forward-item" data-type="channel" data-id="' + srv.id + ':' + ch.id + '">' +
          '<span class="channel-icon">#</span>' +
          '<span>' + escapeHtml(srv.name) + ' / ' + escapeHtml(ch.name) + '</span></div>';
      });
    });
    
    list.innerHTML = h;
    
    list.querySelectorAll('.forward-item').forEach(function(item) {
      item.onclick = function() {
        send({
          type: 'forward_message',
          messageId: state.forwardingMessage.id,
          targetType: item.dataset.type,
          targetId: item.dataset.id,
          originalServerId: state.currentServer,
          originalChannelId: state.currentChannel
        });
        closeModal('forward-modal');
        showNotification('Сообщение переслано');
      };
    });
    
    openModal('forward-modal');
  }

  
  // Server settings
  qS('#save-server-settings').onclick = function() {
    var name = qS('#edit-server-name').value.trim();
    if (name && state.editingServerId) {
      send({ type: 'update_server', serverId: state.editingServerId, name: name, icon: state.editServerIcon });
    }
  };
  
  qS('#delete-server-btn').onclick = function() {
    var srv = state.servers.get(state.editingServerId);
    if (srv) {
      qS('#delete-server-name').textContent = srv.name;
      openModal('confirm-delete-modal');
    }
  };
  
  qS('#confirm-server-name').oninput = function() {
    var srv = state.servers.get(state.editingServerId);
    var btn = qS('#confirm-delete-btn');
    btn.disabled = qS('#confirm-server-name').value !== srv?.name;
  };
  
  qS('#confirm-delete-btn').onclick = function() {
    send({ type: 'delete_server', serverId: state.editingServerId });
    closeModal('confirm-delete-modal');
    closeModal('server-settings-modal');
  };
  
  // Copy invite
  qS('#copy-invite').onclick = function() {
    var code = qS('#invite-code-display').value;
    navigator.clipboard.writeText(code);
    showNotification('Код скопирован');
  };
  
  // Voice controls
  var voiceLeaveBtn = qS('#voice-leave');
  if (voiceLeaveBtn) {
    voiceLeaveBtn.onclick = function() {
      console.log('Leave voice channel clicked');
      leaveVoiceChannel();
    };
  } else {
    console.error('Voice leave button not found');
  }
  
  var voiceMicBtn = qS('#voice-mic');
  if (voiceMicBtn) {
    voiceMicBtn.onclick = function() {
      var muted = toggleMute();
      voiceMicBtn.classList.toggle('muted', muted);
      
      // Toggle icons
      var micOn = voiceMicBtn.querySelector('.mic-on');
      var micOff = voiceMicBtn.querySelector('.mic-off');
      if (micOn && micOff) {
        if (muted) {
          micOn.style.display = 'none';
          micOff.style.display = 'block';
        } else {
          micOn.style.display = 'block';
          micOff.style.display = 'none';
        }
      }
    };
  }
  
  // Video toggle
  var voiceVideoBtn = qS('#voice-video');
  if (voiceVideoBtn) {
    voiceVideoBtn.onclick = function() {
      state.videoEnabled = !state.videoEnabled;
      voiceVideoBtn.classList.toggle('active', state.videoEnabled);
      send({ type: 'voice_video', video: state.videoEnabled });
      showNotification(state.videoEnabled ? 'Видео включено' : 'Видео выключено');
    };
  }
  
  // Screen share toggle
  var voiceScreenBtn = qS('#voice-screen');
  if (voiceScreenBtn) {
    voiceScreenBtn.onclick = function() {
      toggleScreenShare();
    };
  }
  
  // Voice mic settings dropdown
  var micSettingsBtn = qS('#voice-mic-settings');
  var micDropdown = qS('#voice-mic-dropdown');
  if (micSettingsBtn && micDropdown) {
    micSettingsBtn.onclick = function(e) {
      e.stopPropagation();
      micDropdown.classList.toggle('visible');
      if (micDropdown.classList.contains('visible')) {
        loadAudioDevices();
      }
    };
  }
  
  // Noise toggle
  var noiseToggle = qS('#voice-noise-toggle');
  if (noiseToggle) {
    noiseToggle.onchange = function() {
      state.noiseSuppressionEnabled = noiseToggle.checked;
      qS('.voice-toggle-label').textContent = noiseToggle.checked ? 'Включено' : 'Выключено';
      if (localStream) {
        applyNoiseSuppression(localStream);
      }
    };
  }
  
  // Noise button in toolbar
  var noiseBtn = qS('#voice-noise');
  if (noiseBtn) {
    noiseBtn.onclick = function() {
      state.noiseSuppressionEnabled = !state.noiseSuppressionEnabled;
      noiseBtn.classList.toggle('active', state.noiseSuppressionEnabled);
      if (noiseToggle) noiseToggle.checked = state.noiseSuppressionEnabled;
      if (localStream) {
        applyNoiseSuppression(localStream);
      }
      showNotification(state.noiseSuppressionEnabled ? 'Шумоподавление включено' : 'Шумоподавление выключено');
    };
  }
  
  // Test mic button
  var testMicBtn = qS('#voice-test-mic');
  if (testMicBtn) {
    testMicBtn.onclick = function() {
      testMicrophone();
    };
  }
  
  // Close dropdown on outside click
  document.addEventListener('click', function(e) {
    if (micDropdown && !micDropdown.contains(e.target) && e.target !== micSettingsBtn) {
      micDropdown.classList.remove('visible');
    }
  });
  
  // Server settings tabs
  qSA('[data-server-settings]').forEach(function(tab) {
    tab.onclick = function() {
      qSA('[data-server-settings]').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('#server-settings-modal .settings-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#server-settings-' + tab.dataset.serverSettings);
      if (panel) panel.classList.add('active');
    };
  });
  
  // Channel settings tabs
  qSA('[data-channel-settings]').forEach(function(tab) {
    tab.onclick = function() {
      qSA('[data-channel-settings]').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('#edit-channel-modal .settings-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#channel-settings-' + tab.dataset.channelSettings);
      if (panel) panel.classList.add('active');
    };
  });
  
  // Delete channel from settings
  var delChFromSettings = qS('#delete-channel-from-settings');
  if (delChFromSettings) {
    delChFromSettings.onclick = function() {
      var srv = state.servers.get(state.currentServer);
      if (srv && state.editingChannelId) {
        var ch = srv.channels.find(function(c) { return c.id === state.editingChannelId; }) ||
                 srv.voiceChannels.find(function(c) { return c.id === state.editingChannelId; });
        if (ch) {
          qS('#delete-channel-name').textContent = ch.name;
          openModal('confirm-delete-channel-modal');
        }
      }
    };
  }
  
  // Avatar upload
  qS('#upload-avatar').onclick = function() { qS('#avatar-input').click(); };
  qS('#avatar-input').onchange = function(e) {
    var file = e.target.files[0];
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function(ev) {
      var avatar = ev.target.result;
      qS('#settings-avatar').innerHTML = '<img src="' + avatar + '">';
      send({ type: 'update_profile', avatar: avatar });
    };
    reader.readAsDataURL(file);
  };
  
  qS('#remove-avatar').onclick = function() {
    qS('#settings-avatar').innerHTML = state.username ? state.username.charAt(0).toUpperCase() : '?';
    send({ type: 'update_profile', avatar: null });
  };
  
  // Server icon upload
  qS('#upload-server-icon').onclick = function() { qS('#server-icon-input').click(); };
  qS('#server-icon-input').onchange = function(e) {
    var file = e.target.files[0];
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function(ev) {
      state.newServerIcon = ev.target.result;
      qS('#new-server-icon').innerHTML = '<img src="' + state.newServerIcon + '">';
    };
    reader.readAsDataURL(file);
  };
  
  qS('#change-server-icon').onclick = function() { qS('#edit-server-icon-input').click(); };
  qS('#edit-server-icon-input').onchange = function(e) {
    var file = e.target.files[0];
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function(ev) {
      state.editServerIcon = ev.target.result;
      qS('#edit-server-icon').innerHTML = '<img src="' + state.editServerIcon + '">';
    };
    reader.readAsDataURL(file);
  };

  
  // Custom select dropdowns
  qSA('.custom-select-trigger').forEach(function(trigger) {
    trigger.onclick = function(e) {
      e.stopPropagation();
      var wrapper = trigger.closest('.custom-select');
      wrapper.classList.toggle('open');
    };
  });
  
  qSA('.custom-select-options').forEach(function(options) {
    options.onclick = function(e) {
      if (e.target.classList.contains('custom-select-option')) {
        var wrapper = options.closest('.custom-select');
        var trigger = wrapper.querySelector('.custom-select-trigger span');
        options.querySelectorAll('.custom-select-option').forEach(function(o) { o.classList.remove('selected'); });
        e.target.classList.add('selected');
        trigger.textContent = e.target.textContent;
        wrapper.classList.remove('open');
      }
    };
  });
  
  // File attachments
  var attachBtn = qS('#attach-btn');
  var fileInput = qS('#file-input');
  if (attachBtn && fileInput) {
    attachBtn.onclick = function() { fileInput.click(); };
    fileInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      
      var reader = new FileReader();
      reader.onload = function(ev) {
        var attachment = {
          type: file.type.startsWith('image/') ? 'image' : 'file',
          url: ev.target.result,
          name: file.name
        };
        
        var text = qS('#msg-input').value.trim() || '';
        var data = {
          type: 'message',
          serverId: state.currentServer,
          channel: state.currentChannel,
          text: text,
          attachments: [attachment]
        };
        send(data);
        qS('#msg-input').value = '';
        fileInput.value = '';
      };
      reader.readAsDataURL(file);
    };
  }
  
  // DM file attachments
  var dmAttachBtn = qS('#dm-attach-btn');
  var dmFileInput = qS('#dm-file-input');
  if (dmAttachBtn && dmFileInput) {
    dmAttachBtn.onclick = function() { dmFileInput.click(); };
    dmFileInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      
      var reader = new FileReader();
      reader.onload = function(ev) {
        var attachment = {
          type: file.type.startsWith('image/') ? 'image' : 'file',
          url: ev.target.result,
          name: file.name
        };
        
        var text = qS('#dm-input').value.trim() || '';
        send({
          type: 'dm',
          to: state.currentDM,
          text: text,
          attachments: [attachment]
        });
        qS('#dm-input').value = '';
        dmFileInput.value = '';
      };
      reader.readAsDataURL(file);
    };
  }
  
  // Mic test
  var micTestBtn = qS('#mic-test-btn');
  var micTestStream = null;
  var micTestContext = null;
  var micTestAudio = null;
  
  if (micTestBtn) {
    micTestBtn.onclick = function() {
      if (micTestStream) {
        micTestStream.getTracks().forEach(function(t) { t.stop(); });
        micTestStream = null;
        if (micTestContext) micTestContext.close();
        micTestContext = null;
        if (micTestAudio) {
          micTestAudio.srcObject = null;
          micTestAudio = null;
        }
        micTestBtn.querySelector('span').textContent = 'Проверить микрофон';
        qS('#mic-level-bar').style.width = '0%';
        return;
      }
      
      navigator.mediaDevices.getUserMedia({ audio: true }).then(function(stream) {
        micTestStream = stream;
        micTestBtn.querySelector('span').textContent = 'Остановить';
        
        // Play audio back to hear yourself
        micTestAudio = new Audio();
        micTestAudio.srcObject = stream;
        micTestAudio.play();
        
        micTestContext = new AudioContext();
        var analyser = micTestContext.createAnalyser();
        var source = micTestContext.createMediaStreamSource(stream);
        source.connect(analyser);
        analyser.fftSize = 256;
        
        var dataArray = new Uint8Array(analyser.frequencyBinCount);
        
        function updateLevel() {
          if (!micTestStream) return;
          analyser.getByteFrequencyData(dataArray);
          var avg = dataArray.reduce(function(a, b) { return a + b; }, 0) / dataArray.length;
          qS('#mic-level-bar').style.width = Math.min(100, avg * 2) + '%';
          requestAnimationFrame(updateLevel);
        }
        updateLevel();
      }).catch(function(e) {
        console.error('Mic test error:', e);
        showNotification('Не удалось получить доступ к микрофону');
      });
    };
  }
  
  // Emoji reactions (quick add)
  document.addEventListener('dblclick', function(e) {
    var msg = e.target.closest('.message');
    if (msg && state.currentServer && state.currentChannel) {
      send({
        type: 'add_reaction',
        serverId: state.currentServer,
        channelId: state.currentChannel,
        messageId: msg.dataset.id,
        emoji: '👍'
      });
    }
  });
  
  // ============ ROLES UI ============
  var createRoleBtn = qS('#create-role-btn');
  if (createRoleBtn) {
    createRoleBtn.onclick = function() {
      state.editingRoleId = null;
      qS('#role-modal-title').textContent = 'Создать роль';
      qS('#role-name-input').value = '';
      qS('#role-color-input').value = '#99aab5';
      qS('#role-color-preview').style.background = '#99aab5';
      resetPermissionCheckboxes();
      openModal('role-modal');
    };
  }
  
  var roleColorInput = qS('#role-color-input');
  if (roleColorInput) {
    roleColorInput.oninput = function() {
      qS('#role-color-preview').style.background = roleColorInput.value;
    };
  }
  
  var saveRoleBtn = qS('#save-role-btn');
  if (saveRoleBtn) {
    saveRoleBtn.onclick = function() {
      var name = qS('#role-name-input').value.trim();
      var color = qS('#role-color-input').value;
      var permissions = getSelectedPermissions();
      
      if (!name) return;
      
      if (state.editingRoleId) {
        send({ type: 'update_role', serverId: state.editingServerId, roleId: state.editingRoleId, name: name, color: color, permissions: permissions });
      } else {
        send({ type: 'create_role', serverId: state.editingServerId, name: name, color: color, permissions: permissions });
      }
      closeModal('role-modal');
    };
  }
  
  function resetPermissionCheckboxes() {
    qS('#perm-send-messages').checked = true;
    qS('#perm-manage-messages').checked = false;
    qS('#perm-manage-channels').checked = false;
    qS('#perm-kick').checked = false;
    qS('#perm-ban').checked = false;
    qS('#perm-manage-roles').checked = false;
  }
  
  function getSelectedPermissions() {
    var perms = [];
    if (qS('#perm-send-messages').checked) perms.push('send_messages');
    if (qS('#perm-manage-messages').checked) perms.push('manage_messages');
    if (qS('#perm-manage-channels').checked) perms.push('manage_channels');
    if (qS('#perm-kick').checked) perms.push('kick');
    if (qS('#perm-ban').checked) perms.push('ban');
    if (qS('#perm-manage-roles').checked) perms.push('manage_roles');
    return perms;
  }
  
  function setPermissionCheckboxes(perms) {
    qS('#perm-send-messages').checked = perms.includes('send_messages');
    qS('#perm-manage-messages').checked = perms.includes('manage_messages');
    qS('#perm-manage-channels').checked = perms.includes('manage_channels');
    qS('#perm-kick').checked = perms.includes('kick');
    qS('#perm-ban').checked = perms.includes('ban');
    qS('#perm-manage-roles').checked = perms.includes('manage_roles');
  }
  
  // ============ MEMBER MANAGEMENT ============
  var assignRoleBtn = qS('#assign-role-btn');
  if (assignRoleBtn) {
    assignRoleBtn.onclick = function() {
      var roleId = qS('#member-role-select').value;
      if (roleId && state.editingMemberId) {
        send({ type: 'assign_role', serverId: state.editingServerId, memberId: state.editingMemberId, roleId: roleId });
        closeModal('member-modal');
        showNotification('Роль назначена');
      }
    };
  }
  
  var kickMemberBtn = qS('#kick-member-btn');
  if (kickMemberBtn) {
    kickMemberBtn.onclick = function() {
      if (state.editingMemberId && confirm('Исключить участника?')) {
        send({ type: 'kick_member', serverId: state.editingServerId, memberId: state.editingMemberId });
        closeModal('member-modal');
      }
    };
  }
  
  var banMemberBtn = qS('#ban-member-btn');
  if (banMemberBtn) {
    banMemberBtn.onclick = function() {
      if (state.editingMemberId && confirm('Забанить участника?')) {
        send({ type: 'ban_member', serverId: state.editingServerId, memberId: state.editingMemberId });
        closeModal('member-modal');
      }
    };
  }
  
  // ============ SEARCH ============
  var searchChannelBtn = qS('#search-channel-btn');
  if (searchChannelBtn) {
    searchChannelBtn.onclick = function() {
      openModal('search-modal');
      qS('#global-search-input').value = '';
      qS('#global-search-results').innerHTML = '';
      qS('#global-search-input').focus();
    };
  }
  
  var globalSearchBtn = qS('#global-search-btn');
  if (globalSearchBtn) {
    globalSearchBtn.onclick = function() {
      var query = qS('#global-search-input').value.trim();
      if (query && state.currentServer) {
        send({ type: 'search_messages', serverId: state.currentServer, query: query });
      }
    };
  }
  
  var globalSearchInput = qS('#global-search-input');
  if (globalSearchInput) {
    globalSearchInput.onkeypress = function(e) {
      if (e.key === 'Enter') {
        var query = globalSearchInput.value.trim();
        if (query && state.currentServer) {
          send({ type: 'search_messages', serverId: state.currentServer, query: query });
        }
      }
    };
  }
  
  // Connect
  connect();
});