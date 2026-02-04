// ============ CONFIG ============
var RENDER_URL = 'wss://discord-clone-ws-1gb0.onrender.com';
// Always use Render server for sync between web and desktop
var WS_URL = RENDER_URL;

// ============ STATE ============
var state = {
  ws: null,
  userId: null,
  username: null,
  userTag: null,
  userAvatar: null,
  userBanner: null,
  userBio: null,
  userStatus: 'online',
  customStatus: null,
  verifiedEmail: null,
  emailVerified: false,
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
  settings: { notifications: true, sounds: true, privacy: 'everyone' },
  pending2FAEmail: null,
  pending2FAPassword: null
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
    loading.classList.remove('visible', 'fade-out');
    // Process pending auth success immediately
    if (pendingAuthSuccess) {
      processAuthSuccess(pendingAuthSuccess);
      pendingAuthSuccess = null;
    }
  }
}

function processAuthSuccess(msg) {
  state.userId = msg.userId;
  state.username = msg.user.name;
  state.userTag = msg.user.tag || null;
  state.userAvatar = msg.user.avatar;
  state.userBanner = msg.user.banner || null;
  state.userBio = msg.user.bio || null;
  state.userStatus = msg.user.status || 'online';
  state.customStatus = msg.user.customStatus;
  state.userCreatedAt = msg.user.createdAt;
  state.verifiedEmail = msg.user.verifiedEmail || null;
  state.emailVerified = msg.user.emailVerified || false;
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
    openServer(firstServer);
  } else {
    // Show friends view
    state.currentServer = null;
    state.currentChannel = null;
    qS('#server-view').classList.remove('active');
    qS('#home-view').classList.add('active');
    qS('#members-panel').classList.remove('visible');
    showView('friends-view');
  }
  
  // Check for pending invite from URL
  var pendingInvite = localStorage.getItem('pendingInvite');
  if (pendingInvite) {
    localStorage.removeItem('pendingInvite');
    setTimeout(function() {
      send({ type: 'use_invite', code: pendingInvite });
    }, 500);
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
      // Process auth immediately without loading screen
      processAuthSuccess(msg);
    },
    
    auth_error: function() {
      hideAuthLoading();
      localStorage.removeItem('session');
      localStorage.removeItem('lastEmail');
      localStorage.removeItem('lastPwd');
      var loginBox = qS('#login-box');
      var login2faBox = qS('#login-2fa-box');
      
      // If 2FA box is visible, show error there
      if (login2faBox && !login2faBox.classList.contains('hidden')) {
        qS('#login-2fa-error').textContent = msg.message || 'Ошибка';
      } else if (loginBox && !loginBox.classList.contains('hidden')) {
        qS('#login-error').textContent = msg.message || 'Ошибка';
      } else {
        qS('#reg-error').textContent = msg.message || 'Ошибка';
      }
    },
    
    login_2fa_required: function() {
      hideAuthLoading();
      // Store credentials for 2FA verification
      state.pending2FAEmail = msg.email;
      state.pending2FAPassword = localStorage.getItem('lastPwd');
      
      // Show 2FA form
      qS('#login-box').classList.add('hidden');
      qS('#register-box').classList.add('hidden');
      qS('#login-2fa-box').classList.remove('hidden');
      qS('#login-2fa-email').textContent = msg.maskedEmail;
      qS('#login-2fa-error').textContent = '';
      
      // Clear code inputs
      qSA('#login-2fa-box .code-input').forEach(function(inp) { inp.value = ''; });
      qS('#login-2fa-box .code-input[data-index="0"]').focus();
      
      // Show dev code if available
      if (msg.devCode) {
        console.log('Login 2FA code:', msg.devCode);
        showNotification('Dev код: ' + msg.devCode);
      }
    },
    
    login_code_resent: function() {
      showNotification('Код отправлен повторно');
      if (msg.devCode) {
        console.log('Login 2FA code:', msg.devCode);
        showNotification('Dev код: ' + msg.devCode);
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
        if (msg.description !== undefined) srv.description = msg.description;
        if (msg.privacy) srv.privacy = msg.privacy;
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
    
    category_created: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        if (!srv.categories) srv.categories = [];
        srv.categories.push(msg.category);
        if (state.currentServer === msg.serverId) renderChannels();
      }
      closeModal('category-modal');
    },
    
    category_updated: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && srv.categories) {
        var cat = srv.categories.find(function(c) { return c.id === msg.categoryId; });
        if (cat) cat.name = msg.name;
        if (state.currentServer === msg.serverId) renderChannels();
      }
    },
    
    category_deleted: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && srv.categories) {
        srv.categories = srv.categories.filter(function(c) { return c.id !== msg.categoryId; });
        if (state.currentServer === msg.serverId) renderChannels();
      }
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
      var baseUrl = window.location.origin + '?invite=';
      var fullLink = baseUrl + msg.code;
      qS('#invite-code-display').value = fullLink;
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
    
    password_changed: function() {
      showNotification('Пароль успешно изменён');
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
      if (state.editingServerId === msg.serverId) {
        renderServerMembersList();
        // Also update people list if that tab is open
        if (typeof renderPeopleList === 'function') {
          renderPeopleList(msg.members);
        }
      }
    },
    
    invites_list: function() {
      if (state.editingServerId === msg.serverId && typeof renderInvitesList === 'function') {
        renderInvitesList(msg.invites);
      }
    },
    
    audit_log: function() {
      if (state.editingServerId === msg.serverId && typeof renderAuditLog === 'function') {
        renderAuditLog(msg.entries);
      }
    },
    
    bans_list: function() {
      if (state.editingServerId === msg.serverId && typeof renderBansList === 'function') {
        renderBansList(msg.bans);
      }
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
    
    roles_reordered: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv && msg.roles) {
        srv.roles = msg.roles;
        if (state.editingServerId === msg.serverId) renderRoles();
        if (state.currentServer === msg.serverId) renderMembers();
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
    
    roles_updated: function() {
      var srv = state.servers.get(msg.serverId);
      if (srv) {
        srv.memberRoles[msg.memberId] = msg.roles;
        if (msg.membersData) {
          srv.membersData = msg.membersData;
        }
        if (state.currentServer === msg.serverId) {
          renderMembers();
        }
      }
    },
    
    voice_state_update: function() {
      // Save voice users for this channel
      state.voiceUsers.set(msg.channelId, msg.users || []);
      
      // Update voice users display
      if (state.currentServer === msg.serverId) {
        renderChannels();
        
        // Render voice users if we're in this channel
        if (state.voiceChannel === msg.channelId) {
          renderVoiceUsers(msg.channelId, msg.users);
        }
        
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
    
    voice_screen_update: function() {
      console.log('Screen update from:', msg.userId, 'screen:', msg.screen);
      // Another user started/stopped screen sharing
      if (msg.userId !== state.userId) {
        if (msg.screen) {
          showNotification('Пользователь начал демонстрацию экрана');
        } else {
          // Remove their screen share window
          var container = document.getElementById('screen-share-container-' + msg.userId);
          if (container) container.remove();
        }
      }
    },
    
    voice_signal: function() {
      if (msg.from && msg.signal) {
        handleVoiceSignal(msg.from, msg.signal);
      }
    },
    
    // DM Call handlers
    dm_call_incoming: function() {
      if (dmCallState.active) {
        // Already in a call, reject
        send({ type: 'dm_call_reject', to: msg.from });
        return;
      }
      var caller = state.friends.get(msg.from);
      var callerName = caller ? caller.name : 'Пользователь';
      var callerAvatar = caller ? caller.avatar : null;
      showIncomingCall(msg.from, callerName, callerAvatar, msg.withVideo);
    },
    
    dm_call_accepted: function() {
      // Other user accepted, create peer connection and start call
      if (dmCallState.active && dmCallState.peerId === msg.from) {
        createDMPeerConnection(msg.from, true);
        var statusEl = qS('#dm-call-status');
        if (statusEl) {
          statusEl.textContent = 'Подключение...';
        }
      }
    },
    
    dm_call_rejected: function() {
      if (dmCallState.active && dmCallState.peerId === msg.from) {
        stopAllCallSounds();
        playBusyTone();
        setTimeout(function() {
          endDMCall();
        }, 1600);
        showNotification('Звонок отклонён');
      }
    },
    
    dm_call_signal: function() {
      if (msg.from && msg.signal) {
        handleDMCallSignal(msg.from, msg.signal);
      }
    },
    
    dm_call_ended: function() {
      if (dmCallState.active && dmCallState.peerId === msg.from) {
        endDMCall();
        showNotification('Звонок завершён');
      }
      // Also close incoming call modal if open
      stopAllCallSounds();
      closeModal('incoming-call-modal');
    },
    
    search_results: function() {
      state.searchResults = msg.results;
      renderSearchResults();
    },
    
    user_search_results: function() {
      renderUserSearchResults(msg.results);
    },
    
    mention: function() {
      // Show notification for mention
      var fromName = msg.from ? msg.from.name : 'Кто-то';
      var serverName = msg.serverName || 'сервер';
      var channelName = msg.channelName || 'канал';
      
      showMentionNotification(fromName, serverName, channelName, msg.message, msg.serverId, msg.channelId);
      
      // Play sound if enabled
      if (state.settings.sounds) {
        playMentionSound();
      }
    },
    
    verification_sent: function() {
      showNotification('Код отправлен на ' + msg.email);
      qS('#email-change-form').classList.add('hidden');
      qS('#email-verify-form').classList.remove('hidden');
      state.pendingEmail = msg.email;
      
      // For dev mode without SMTP
      if (msg.devCode) {
        console.log('Dev verification code:', msg.devCode);
        showNotification('Dev код: ' + msg.devCode);
      }
    },
    
    verification_error: function() {
      showNotification(msg.message || 'Ошибка верификации');
      // Shake code inputs if error
      qSA('.code-input').forEach(function(inp) {
        inp.classList.add('error');
        setTimeout(function() { inp.classList.remove('error'); }, 300);
      });
    },
    
    email_verified: function() {
      showNotification('Email успешно подтверждён!');
      state.verifiedEmail = msg.email;
      state.emailVerified = true;
      updateEmailStatus();
      qS('#email-verify-form').classList.add('hidden');
      qS('#email-change-form').classList.remove('hidden');
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

function showMentionNotification(fromName, serverName, channelName, message, serverId, channelId) {
  var n = document.createElement('div');
  n.className = 'mention-notification';
  n.innerHTML = 
    '<div class="mention-notification-header">' +
      '<span class="mention-icon">@</span>' +
      '<span class="mention-title">Вас упомянули</span>' +
      '<button class="mention-close">&times;</button>' +
    '</div>' +
    '<div class="mention-notification-body">' +
      '<div class="mention-from"><strong>' + escapeHtml(fromName) + '</strong> в <span class="mention-channel">#' + escapeHtml(channelName) + '</span></div>' +
      '<div class="mention-text">' + escapeHtml(message.text).substring(0, 100) + (message.text.length > 100 ? '...' : '') + '</div>' +
    '</div>';
  
  document.body.appendChild(n);
  
  // Close button
  n.querySelector('.mention-close').onclick = function(e) {
    e.stopPropagation();
    n.classList.remove('show');
    setTimeout(function() { n.remove(); }, 300);
  };
  
  // Click to go to message
  n.onclick = function() {
    if (serverId && channelId) {
      openServer(serverId);
      setTimeout(function() {
        openChannel(channelId);
      }, 100);
    }
    n.classList.remove('show');
    setTimeout(function() { n.remove(); }, 300);
  };
  
  setTimeout(function() { n.classList.add('show'); }, 10);
  
  // Auto hide after 8 seconds
  setTimeout(function() {
    if (n.parentNode) {
      n.classList.remove('show');
      setTimeout(function() { n.remove(); }, 300);
    }
  }, 8000);
}

function playMentionSound() {
  try {
    var audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    var oscillator = audioCtx.createOscillator();
    var gainNode = audioCtx.createGain();
    
    oscillator.connect(gainNode);
    gainNode.connect(audioCtx.destination);
    
    oscillator.frequency.value = 800;
    oscillator.type = 'sine';
    gainNode.gain.setValueAtTime(0.3, audioCtx.currentTime);
    gainNode.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.3);
    
    oscillator.start(audioCtx.currentTime);
    oscillator.stop(audioCtx.currentTime + 0.3);
  } catch (e) {
    console.log('Could not play mention sound');
  }
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
  
  // Update my tag display
  var myTagEl = qS('#my-user-tag');
  if (myTagEl) myTagEl.textContent = '#' + (state.userTag || '0000');
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
  var roles = srv.roles || [];
  if (!srv.memberRoles) srv.memberRoles = {};
  
  // Group members by their highest hoisted role
  var roleGroups = {};
  var ungroupedOnline = [];
  var ungroupedOffline = [];
  
  // Track members in hoisted roles
  var membersInHoistedRoles = new Set();
  
  mems.forEach(function(m) {
    // Support multiple roles
    var memberRoleIds = srv.memberRoles[m.id] || ['default'];
    if (!Array.isArray(memberRoleIds)) memberRoleIds = [memberRoleIds];
    
    // Find highest hoisted role
    var memberRoles = memberRoleIds.map(function(rid) {
      return roles.find(function(r) { return r.id === rid; });
    }).filter(Boolean);
    
    // Sort by position to get highest
    memberRoles.sort(function(a, b) { return (b.position || 0) - (a.position || 0); });
    
    // Find first hoisted role
    var hoistedRole = memberRoles.find(function(r) { return r.hoist; });
    
    // Store all roles on member for display
    m.roleData = memberRoles[0] || null;
    m.allRoles = memberRoles;
    
    var isOnline = m.status === 'online';
    
    // If member has a hoisted role, group by that role
    if (hoistedRole) {
      if (!roleGroups[hoistedRole.id]) {
        roleGroups[hoistedRole.id] = { role: hoistedRole, members: [] };
      }
      roleGroups[hoistedRole.id].members.push(m);
      membersInHoistedRoles.add(m.id);
    } else if (isOnline) {
      ungroupedOnline.push(m);
    } else {
      ungroupedOffline.push(m);
    }
  });
  
  // Build HTML - first show role groups, then online, then offline
  var html = '';
  
  // Sort roles by position (higher position = higher priority, show first)
  var sortedRoles = Object.values(roleGroups).sort(function(a, b) { 
    return (b.role.position || 0) - (a.role.position || 0); 
  });
  
  sortedRoles.forEach(function(group) {
    html += '<div class="member-group">';
    html += '<div class="member-group-header">' + 
      escapeHtml(group.role.name).toUpperCase() + ' — ' + group.members.length + '</div>';
    html += group.members.map(function(m) { return memberHTML(m, srv); }).join('');
    html += '</div>';
  });
  
  // Show "В СЕТИ" section only with ungrouped online members (not in hoisted roles)
  if (ungroupedOnline.length > 0) {
    html += '<div class="member-group">';
    html += '<div class="member-group-header">В СЕТИ — ' + ungroupedOnline.length + '</div>';
    html += ungroupedOnline.map(function(m) { return memberHTML(m, srv); }).join('');
    html += '</div>';
  }
  
  ol.innerHTML = html;
  
  // Show offline members (not in hoisted roles)
  var offlineHtml = '';
  if (ungroupedOffline.length > 0) {
    offlineHtml = '<div class="member-group-header">НЕ В СЕТИ — ' + ungroupedOffline.length + '</div>';
    offlineHtml += ungroupedOffline.map(function(m) { return memberHTML(m, srv); }).join('');
  }
  ofl.innerHTML = offlineHtml;
  
  // Bind member context menu
  qSA('.member-item').forEach(function(el) {
    el.oncontextmenu = function(e) {
      e.preventDefault();
      showMemberContext(e.clientX, e.clientY, el.dataset.id);
    };
  });
}

function memberHTML(m, srv) {
  var crown = m.isOwner ? '<svg class="crown-icon" viewBox="0 0 24 24" fill="currentColor"><path d="M5 16L3 5l5.5 5L12 4l3.5 6L21 5l-2 11H5zm14 3c0 .6-.4 1-1 1H6c-.6 0-1-.4-1-1v-1h14v1z"/></svg>' : '';
  
  // Get role color for name
  var nameColor = '';
  var roleBadge = '';
  if (m.roleData) {
    nameColor = ' style="color: ' + (m.roleData.color || 'inherit') + '"';
    roleBadge = '<span class="role-badge" style="background: ' + (m.roleData.color || '#99aab5') + '20; color: ' + (m.roleData.color || '#99aab5') + '; border-color: ' + (m.roleData.color || '#99aab5') + '40">' + escapeHtml(m.roleData.name) + '</span>';
  }
  
  return '<div class="member-item" data-id="' + m.id + '">' +
    '<div class="avatar ' + (m.status || 'offline') + '">' + (m.avatar ? '<img src="' + m.avatar + '">' : (m.name ? m.name.charAt(0).toUpperCase() : '?')) + '</div>' +
    '<span class="member-name"' + nameColor + '>' + escapeHtml(m.name || 'User') + crown + '</span>' + roleBadge + '</div>';
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
  if (!vu) return;
  
  // Only render if we're in this channel
  if (state.voiceChannel !== channelId) return;
  
  // Save screen share if exists
  var screenShare = qS('#local-screen-preview-container');
  var screenShareHTML = screenShare ? screenShare.outerHTML : '';
  var screenShareElement = screenShare;
  
  // Render users
  var usersHTML = (users || []).map(function(u) {
    return '<div class="voice-user" data-user-id="' + u.id + '">' +
      '<div class="avatar" data-user-id="' + u.id + '">' + (u.avatar ? '<img src="' + u.avatar + '">' : (u.name ? u.name.charAt(0).toUpperCase() : '?')) + '</div>' +
      '<span>' + escapeHtml(u.name) + '</span>' +
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
  
  // Sort roles by position (higher position = higher priority)
  var sortedRoles = srv.roles.slice().sort(function(a, b) {
    return (b.position || 0) - (a.position || 0);
  });
  
  rl.innerHTML = sortedRoles.map(function(role) {
    var isDefault = role.id === 'default';
    var memberCount = Object.values(srv.memberRoles || {}).filter(function(r) { return r === role.id; }).length;
    if (role.id === 'default') {
      memberCount = (srv.members ? srv.members.length : 0) - Object.keys(srv.memberRoles || {}).length;
      if (memberCount < 0) memberCount = 0;
    }
    
    return '<div class="role-item' + (isDefault ? '' : ' draggable') + '" data-id="' + role.id + '" ' + (isDefault ? '' : 'draggable="true"') + '>' +
      (isDefault ? '' : '<div class="drag-handle"><svg viewBox="0 0 24 24" fill="currentColor"><circle cx="9" cy="6" r="1.5"/><circle cx="15" cy="6" r="1.5"/><circle cx="9" cy="12" r="1.5"/><circle cx="15" cy="12" r="1.5"/><circle cx="9" cy="18" r="1.5"/><circle cx="15" cy="18" r="1.5"/></svg></div>') +
      '<div class="role-info">' +
      '<div class="role-color" style="background: ' + (role.color || '#99aab5') + '"></div>' +
      '<div class="role-details">' +
      '<span class="role-name" style="color: ' + (role.color || 'inherit') + '">' + escapeHtml(role.name) + '</span>' +
      '<span class="role-member-count">' + memberCount + ' участник' + (memberCount === 1 ? '' : memberCount < 5 ? 'а' : 'ов') + '</span>' +
      '</div>' +
      '</div>' +
      '<div class="role-actions">' +
      (isDefault ? '' : '<button class="btn secondary edit-role-btn" data-id="' + role.id + '">Изменить</button>') +
      (isDefault ? '' : '<button class="btn danger delete-role-btn" data-id="' + role.id + '">Удалить</button>') +
      '</div></div>';
  }).join('');
  
  // Setup drag and drop
  setupRoleDragAndDrop(rl);
  
  rl.querySelectorAll('.edit-role-btn').forEach(function(btn) {
    btn.onclick = function(e) {
      e.stopPropagation();
      var role = srv.roles.find(function(r) { return r.id === btn.dataset.id; });
      if (role) {
        state.editingRoleId = role.id;
        qS('#role-modal-title').textContent = 'Редактировать роль';
        qS('#role-name-input').value = role.name;
        qS('#role-color-input').value = role.color || '#99aab5';
        var hexInput = qS('#role-color-hex');
        if (hexInput) hexInput.value = role.color || '#99aab5';
        qS('#role-color-preview').style.background = role.color || '#99aab5';
        // Set color preset active
        qSA('.color-preset').forEach(function(p) { 
          p.classList.toggle('active', p.dataset.color === (role.color || '#99aab5'));
        });
        // Set hoist and mentionable
        var hoistCheckbox = qS('#role-hoist');
        if (hoistCheckbox) hoistCheckbox.checked = role.hoist || false;
        var mentionableCheckbox = qS('#role-mentionable');
        if (mentionableCheckbox) mentionableCheckbox.checked = role.mentionable || false;
        // Set role icon
        var iconPreview = qS('#role-icon-preview');
        if (iconPreview) {
          if (role.icon) {
            iconPreview.innerHTML = '<img src="' + role.icon + '">';
            state.roleIcon = role.icon;
          } else {
            iconPreview.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>';
            state.roleIcon = null;
          }
        }
        setPermissionCheckboxes(role.permissions || []);
        // Reset to first tab
        qSA('.role-tab').forEach(function(t) { t.classList.remove('active'); });
        qS('.role-tab[data-role-tab="general"]').classList.add('active');
        qSA('.role-panel').forEach(function(p) { p.classList.remove('active'); });
        qS('#role-panel-general').classList.add('active');
        openModal('role-modal');
      }
    };
  });
  
  rl.querySelectorAll('.delete-role-btn').forEach(function(btn) {
    btn.onclick = function(e) {
      e.stopPropagation();
      var roleId = btn.dataset.id;
      showConfirmDialog('Удалить роль?', function() {
        send({ type: 'delete_role', serverId: state.editingServerId, roleId: roleId });
      });
    };
  });
}

function setupRoleDragAndDrop(container) {
  var draggedItem = null;
  var placeholder = null;
  
  container.querySelectorAll('.role-item.draggable').forEach(function(item) {
    item.addEventListener('dragstart', function(e) {
      draggedItem = item;
      item.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', item.dataset.id);
      
      // Create placeholder
      placeholder = document.createElement('div');
      placeholder.className = 'role-item-placeholder';
      placeholder.style.height = item.offsetHeight + 'px';
    });
    
    item.addEventListener('dragend', function() {
      item.classList.remove('dragging');
      if (placeholder && placeholder.parentNode) {
        placeholder.parentNode.removeChild(placeholder);
      }
      draggedItem = null;
      placeholder = null;
    });
    
    item.addEventListener('dragover', function(e) {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      
      if (!draggedItem || draggedItem === item) return;
      if (item.dataset.id === 'default') return;
      
      var rect = item.getBoundingClientRect();
      var midY = rect.top + rect.height / 2;
      
      if (e.clientY < midY) {
        item.parentNode.insertBefore(placeholder, item);
      } else {
        item.parentNode.insertBefore(placeholder, item.nextSibling);
      }
    });
    
    item.addEventListener('drop', function(e) {
      e.preventDefault();
      if (!draggedItem || draggedItem === item) return;
      
      // Insert dragged item at placeholder position
      if (placeholder && placeholder.parentNode) {
        placeholder.parentNode.insertBefore(draggedItem, placeholder);
        placeholder.parentNode.removeChild(placeholder);
      }
      
      // Update positions based on new order
      updateRolePositions(container);
    });
  });
  
  // Allow dropping on container
  container.addEventListener('dragover', function(e) {
    e.preventDefault();
  });
  
  container.addEventListener('drop', function(e) {
    e.preventDefault();
    if (placeholder && placeholder.parentNode) {
      placeholder.parentNode.insertBefore(draggedItem, placeholder);
      placeholder.parentNode.removeChild(placeholder);
    }
    updateRolePositions(container);
  });
}

function updateRolePositions(container) {
  var items = container.querySelectorAll('.role-item');
  var positions = [];
  var totalItems = items.length;
  
  items.forEach(function(item, index) {
    var roleId = item.dataset.id;
    if (roleId && roleId !== 'default') {
      // Higher index in DOM = lower position (since we sort desc)
      // First item should have highest position
      var newPosition = (totalItems - index) * 10;
      positions.push({ roleId: roleId, position: newPosition });
    }
  });
  
  // Send update to server
  if (positions.length > 0) {
    send({ 
      type: 'update_role_positions', 
      serverId: state.editingServerId, 
      positions: positions 
    });
    showNotification('Порядок ролей обновлён');
  }
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
    select.innerHTML = srv.roles.map(function(r) {
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
      
      // Play join sound
      playVoiceJoin();
      
      // Setup audio analyser for local user speaking detection
      setupAudioAnalyser(stream, state.userId);
      startSpeakingDetection();
      
      // Update noise button state
      var noiseBtn = qS('#voice-noise');
      if (noiseBtn) noiseBtn.classList.toggle('active', state.noiseSuppressionEnabled);
      
      // Reset screen share button state
      var screenBtn = qS('#voice-screen');
      if (screenBtn) screenBtn.classList.remove('active');
      state.screenSharing = false;
      
      // Reset mic button state (unmuted by default)
      var micBtn = qS('#voice-mic');
      if (micBtn) {
        micBtn.classList.remove('muted');
        var micOn = micBtn.querySelector('.mic-on');
        var micOff = micBtn.querySelector('.mic-off');
        if (micOn) micOn.style.display = 'block';
        if (micOff) micOff.style.display = 'none';
      }
      
      // Reset user panel mic button state
      var userMicBtn = qS('#mic-toggle');
      if (userMicBtn) {
        userMicBtn.classList.remove('muted');
      }
      
      // Optimistic update - add self to voice users immediately
      var currentUsers = state.voiceUsers.get(id) || [];
      var selfInList = currentUsers.some(function(u) { return u.id === state.userId; });
      if (!selfInList) {
        currentUsers.push({
          id: state.userId,
          name: state.username,
          avatar: state.userAvatar,
          muted: false,
          video: false,
          screen: false
        });
        state.voiceUsers.set(id, currentUsers);
      }
      
      send({ type: 'voice_join', channelId: id, serverId: state.currentServer });
      renderChannels();
      
      var srv = state.servers.get(state.currentServer);
      var ch = srv ? srv.voiceChannels.find(function(c) { return c.id === id; }) : null;
      qS('#voice-name').textContent = ch ? ch.name : 'Голосовой';
      showView('voice-view');
      
      // Render voice users with self included
      renderVoiceUsers(id, state.voiceUsers.get(id));
    })
    .catch(function(err) {
      console.error('Microphone error:', err);
      showNotification('Не удалось получить доступ к микрофону');
      state.voiceChannel = null;
    });
}

function leaveVoiceChannel() {
  // Play leave sound
  playVoiceLeave();
  
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
  
  // Clear voice users for this channel
  var channelId = state.voiceChannel;
  if (channelId) {
    state.voiceUsers.delete(channelId);
  }
  
  send({ type: 'voice_leave', channelId: state.voiceChannel });
  state.voiceChannel = null;
  renderChannels();
  showView('chat-view');
}

// DM Call state
var dmCallState = {
  active: false,
  peerId: null,
  peerConnection: null,
  localStream: null,
  remoteStream: null,
  isMuted: false,
  isVideoEnabled: false,
  isIncoming: false,
  callTimer: null,
  callDuration: 0,
  ringtoneInterval: null,
  dialingInterval: null
};

// Call sounds using Web Audio API
var callSoundContext = null;

function getAudioContext() {
  if (!callSoundContext) {
    callSoundContext = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (callSoundContext.state === 'suspended') {
    callSoundContext.resume();
  }
  return callSoundContext;
}

// Musical ringtone - pleasant melody
function playRingtone() {
  stopAllCallSounds();
  var ctx = getAudioContext();
  
  // Musical notes (C major arpeggio pattern)
  var melody = [
    { freq: 523.25, dur: 0.15 },  // C5
    { freq: 659.25, dur: 0.15 },  // E5
    { freq: 783.99, dur: 0.15 },  // G5
    { freq: 1046.50, dur: 0.3 },  // C6
    { freq: 783.99, dur: 0.15 },  // G5
    { freq: 659.25, dur: 0.15 },  // E5
  ];
  
  function playMelody() {
    var time = ctx.currentTime;
    
    melody.forEach(function(note) {
      var osc = ctx.createOscillator();
      var gain = ctx.createGain();
      
      osc.type = 'sine';
      osc.frequency.value = note.freq;
      
      gain.gain.setValueAtTime(0, time);
      gain.gain.linearRampToValueAtTime(0.15, time + 0.02);
      gain.gain.setValueAtTime(0.15, time + note.dur - 0.03);
      gain.gain.linearRampToValueAtTime(0, time + note.dur);
      
      osc.connect(gain);
      gain.connect(ctx.destination);
      
      osc.start(time);
      osc.stop(time + note.dur);
      
      time += note.dur;
    });
  }
  
  playMelody();
  dmCallState.ringtoneInterval = setInterval(playMelody, 1800);
}

// Dialing tone - soft pulsing
function playDialingTone() {
  stopAllCallSounds();
  var ctx = getAudioContext();
  
  function playBeep() {
    var osc = ctx.createOscillator();
    var osc2 = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc2.type = 'sine';
    osc.frequency.value = 440;
    osc2.frequency.value = 480;
    
    gain.gain.setValueAtTime(0, ctx.currentTime);
    gain.gain.linearRampToValueAtTime(0.08, ctx.currentTime + 0.1);
    gain.gain.setValueAtTime(0.08, ctx.currentTime + 0.9);
    gain.gain.linearRampToValueAtTime(0, ctx.currentTime + 1);
    
    osc.connect(gain);
    osc2.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start();
    osc2.start();
    
    setTimeout(function() {
      osc.stop();
      osc2.stop();
    }, 1000);
  }
  
  playBeep();
  dmCallState.dialingInterval = setInterval(playBeep, 3500);
}

// Call connected - pleasant chime
function playCallConnected() {
  stopAllCallSounds();
  var ctx = getAudioContext();
  var time = ctx.currentTime;
  
  // Two-note ascending chime
  var notes = [
    { freq: 880, start: 0, dur: 0.15 },
    { freq: 1318.51, start: 0.1, dur: 0.25 }
  ];
  
  notes.forEach(function(note) {
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = note.freq;
    
    gain.gain.setValueAtTime(0, time + note.start);
    gain.gain.linearRampToValueAtTime(0.12, time + note.start + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, time + note.start + note.dur);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start(time + note.start);
    osc.stop(time + note.start + note.dur);
  });
}

// Call ended / disconnect - descending tone
function playCallEnded() {
  stopAllCallSounds();
  var ctx = getAudioContext();
  var time = ctx.currentTime;
  
  // Two descending notes
  var notes = [
    { freq: 659.25, start: 0, dur: 0.15 },
    { freq: 440, start: 0.12, dur: 0.2 }
  ];
  
  notes.forEach(function(note) {
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = note.freq;
    
    gain.gain.setValueAtTime(0, time + note.start);
    gain.gain.linearRampToValueAtTime(0.1, time + note.start + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, time + note.start + note.dur);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start(time + note.start);
    osc.stop(time + note.start + note.dur);
  });
}

// Busy tone
function playBusyTone() {
  stopAllCallSounds();
  var ctx = getAudioContext();
  
  var count = 0;
  function playBusy() {
    if (count >= 4) return;
    count++;
    
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = 480;
    
    gain.gain.setValueAtTime(0.1, ctx.currentTime);
    gain.gain.setValueAtTime(0.1, ctx.currentTime + 0.2);
    gain.gain.linearRampToValueAtTime(0, ctx.currentTime + 0.25);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start();
    setTimeout(function() { osc.stop(); }, 250);
  }
  
  playBusy();
  var busyInterval = setInterval(function() {
    playBusy();
    if (count >= 4) clearInterval(busyInterval);
  }, 400);
}

// Voice channel join sound
function playVoiceJoin() {
  var ctx = getAudioContext();
  var time = ctx.currentTime;
  
  // Ascending two-note chime
  var notes = [
    { freq: 523.25, start: 0, dur: 0.12 },
    { freq: 783.99, start: 0.08, dur: 0.18 }
  ];
  
  notes.forEach(function(note) {
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = note.freq;
    
    gain.gain.setValueAtTime(0, time + note.start);
    gain.gain.linearRampToValueAtTime(0.1, time + note.start + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, time + note.start + note.dur);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start(time + note.start);
    osc.stop(time + note.start + note.dur);
  });
}

// Voice channel leave sound
function playVoiceLeave() {
  var ctx = getAudioContext();
  var time = ctx.currentTime;
  
  // Descending two-note
  var notes = [
    { freq: 783.99, start: 0, dur: 0.12 },
    { freq: 523.25, start: 0.08, dur: 0.18 }
  ];
  
  notes.forEach(function(note) {
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = note.freq;
    
    gain.gain.setValueAtTime(0, time + note.start);
    gain.gain.linearRampToValueAtTime(0.08, time + note.start + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, time + note.start + note.dur);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start(time + note.start);
    osc.stop(time + note.start + note.dur);
  });
}

// Mute sound - low click
function playMuteSound() {
  var ctx = getAudioContext();
  
  var osc = ctx.createOscillator();
  var gain = ctx.createGain();
  
  osc.type = 'sine';
  osc.frequency.value = 350;
  
  gain.gain.setValueAtTime(0.08, ctx.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.08);
  
  osc.connect(gain);
  gain.connect(ctx.destination);
  
  osc.start();
  setTimeout(function() { osc.stop(); }, 100);
}

// Unmute sound - higher click
function playUnmuteSound() {
  var ctx = getAudioContext();
  
  var osc = ctx.createOscillator();
  var gain = ctx.createGain();
  
  osc.type = 'sine';
  osc.frequency.value = 500;
  
  gain.gain.setValueAtTime(0.08, ctx.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.08);
  
  osc.connect(gain);
  gain.connect(ctx.destination);
  
  osc.start();
  setTimeout(function() { osc.stop(); }, 100);
}

// Screen share start sound - notification chime
function playScreenShareStart() {
  var ctx = getAudioContext();
  var time = ctx.currentTime;
  
  // Three ascending notes
  var notes = [
    { freq: 587.33, start: 0, dur: 0.1 },     // D5
    { freq: 739.99, start: 0.08, dur: 0.1 },  // F#5
    { freq: 880, start: 0.16, dur: 0.2 }      // A5
  ];
  
  notes.forEach(function(note) {
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = note.freq;
    
    gain.gain.setValueAtTime(0, time + note.start);
    gain.gain.linearRampToValueAtTime(0.1, time + note.start + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, time + note.start + note.dur);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start(time + note.start);
    osc.stop(time + note.start + note.dur);
  });
}

// Screen share stop sound
function playScreenShareStop() {
  var ctx = getAudioContext();
  var time = ctx.currentTime;
  
  // Two descending notes
  var notes = [
    { freq: 739.99, start: 0, dur: 0.1 },
    { freq: 523.25, start: 0.08, dur: 0.15 }
  ];
  
  notes.forEach(function(note) {
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    
    osc.type = 'sine';
    osc.frequency.value = note.freq;
    
    gain.gain.setValueAtTime(0, time + note.start);
    gain.gain.linearRampToValueAtTime(0.08, time + note.start + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, time + note.start + note.dur);
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    
    osc.start(time + note.start);
    osc.stop(time + note.start + note.dur);
  });
}

function stopAllCallSounds() {
  if (dmCallState.ringtoneInterval) {
    clearInterval(dmCallState.ringtoneInterval);
    dmCallState.ringtoneInterval = null;
  }
  if (dmCallState.dialingInterval) {
    clearInterval(dmCallState.dialingInterval);
    dmCallState.dialingInterval = null;
  }
}

// DM Call function
function startDMCall(userId, withVideo) {
  if (!userId) return;
  if (dmCallState.active) {
    showNotification('Вы уже в звонке');
    return;
  }
  
  var friend = state.friends.get(userId);
  var friendName = friend ? friend.name : 'Пользователь';
  var friendAvatar = friend ? friend.avatar : null;
  
  // Setup call UI
  var avatarEl = qS('#dm-call-avatar');
  var nameEl = qS('#dm-call-name');
  var statusEl = qS('#dm-call-status');
  
  if (avatarEl) {
    if (friendAvatar) {
      avatarEl.innerHTML = '<img src="' + friendAvatar + '">';
    } else {
      avatarEl.textContent = friendName.charAt(0).toUpperCase();
    }
  }
  if (nameEl) nameEl.textContent = friendName;
  if (statusEl) {
    statusEl.textContent = 'Вызов...';
    statusEl.classList.remove('connected');
  }
  
  dmCallState.isVideoEnabled = withVideo;
  dmCallState.peerId = userId;
  
  // Get media
  navigator.mediaDevices.getUserMedia({
    audio: { echoCancellation: true, noiseSuppression: true },
    video: withVideo
  }).then(function(stream) {
    dmCallState.localStream = stream;
    dmCallState.active = true;
    
    // Play dialing tone
    playDialingTone();
    
    // Show local video if video call
    if (withVideo) {
      var localVideo = qS('#dm-local-video');
      if (localVideo) {
        localVideo.srcObject = stream;
      }
      qS('#dm-call-video-container').classList.add('active');
    }
    
    // Send call request
    send({ type: 'dm_call_request', to: userId, withVideo: withVideo });
    
    openModal('dm-call-modal');
    
    // Timeout for no answer
    setTimeout(function() {
      if (dmCallState.active && !dmCallState.peerConnection) {
        playBusyTone();
        setTimeout(function() {
          endDMCall();
          showNotification(friendName + ' не отвечает');
        }, 1600);
      }
    }, 30000);
    
  }).catch(function(err) {
    console.error('Media error:', err);
    showNotification('Не удалось получить доступ к микрофону');
  });
}

function acceptDMCall(fromId, withVideo) {
  // Stop ringtone
  stopAllCallSounds();
  
  var friend = state.friends.get(fromId);
  var friendName = friend ? friend.name : 'Пользователь';
  var friendAvatar = friend ? friend.avatar : null;
  
  closeModal('incoming-call-modal');
  
  // Setup call UI
  var avatarEl = qS('#dm-call-avatar');
  var nameEl = qS('#dm-call-name');
  var statusEl = qS('#dm-call-status');
  
  if (avatarEl) {
    if (friendAvatar) {
      avatarEl.innerHTML = '<img src="' + friendAvatar + '">';
    } else {
      avatarEl.textContent = friendName.charAt(0).toUpperCase();
    }
  }
  if (nameEl) nameEl.textContent = friendName;
  if (statusEl) {
    statusEl.textContent = 'Подключение...';
    statusEl.classList.remove('connected');
  }
  
  dmCallState.peerId = fromId;
  dmCallState.isVideoEnabled = withVideo;
  dmCallState.isIncoming = true;
  
  navigator.mediaDevices.getUserMedia({
    audio: { echoCancellation: true, noiseSuppression: true },
    video: withVideo
  }).then(function(stream) {
    dmCallState.localStream = stream;
    dmCallState.active = true;
    
    if (withVideo) {
      var localVideo = qS('#dm-local-video');
      if (localVideo) localVideo.srcObject = stream;
      qS('#dm-call-video-container').classList.add('active');
    }
    
    // Send accept
    send({ type: 'dm_call_accept', to: fromId, withVideo: withVideo });
    
    openModal('dm-call-modal');
    
    // Create peer connection
    createDMPeerConnection(fromId, false);
    
  }).catch(function(err) {
    console.error('Media error:', err);
    showNotification('Не удалось получить доступ к микрофону');
    send({ type: 'dm_call_reject', to: fromId });
  });
}

function createDMPeerConnection(peerId, isInitiator) {
  var pc = new RTCPeerConnection(rtcConfig);
  dmCallState.peerConnection = pc;
  
  // Add local tracks
  if (dmCallState.localStream) {
    dmCallState.localStream.getTracks().forEach(function(track) {
      pc.addTrack(track, dmCallState.localStream);
    });
  }
  
  // Handle remote tracks
  pc.ontrack = function(event) {
    console.log('DM Call: received remote track', event.track.kind);
    var remoteVideo = qS('#dm-remote-video');
    if (remoteVideo && event.streams[0]) {
      remoteVideo.srcObject = event.streams[0];
      dmCallState.remoteStream = event.streams[0];
    }
    
    // If audio track, also create audio element
    if (event.track.kind === 'audio') {
      var audio = document.createElement('audio');
      audio.id = 'dm-call-audio';
      audio.srcObject = event.streams[0];
      audio.autoplay = true;
      document.body.appendChild(audio);
    }
    
    // Update status
    var statusEl = qS('#dm-call-status');
    if (statusEl) {
      statusEl.textContent = 'Подключено';
      statusEl.classList.add('connected');
    }
    
    // Start call timer
    startCallTimer();
  };
  
  // ICE candidates
  pc.onicecandidate = function(event) {
    if (event.candidate) {
      send({ type: 'dm_call_signal', to: peerId, signal: { type: 'candidate', candidate: event.candidate } });
    }
  };
  
  pc.onconnectionstatechange = function() {
    console.log('DM Call connection state:', pc.connectionState);
    if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed') {
      endDMCall();
      showNotification('Звонок прерван');
    }
  };
  
  // If initiator, create offer
  if (isInitiator) {
    pc.createOffer().then(function(offer) {
      return pc.setLocalDescription(offer);
    }).then(function() {
      send({ type: 'dm_call_signal', to: peerId, signal: { type: 'offer', sdp: pc.localDescription } });
    }).catch(function(err) {
      console.error('Offer error:', err);
    });
  }
}

function handleDMCallSignal(fromId, signal) {
  if (!dmCallState.active || dmCallState.peerId !== fromId) return;
  
  var pc = dmCallState.peerConnection;
  if (!pc) {
    // Create peer connection if not exists (we're the callee)
    createDMPeerConnection(fromId, false);
    pc = dmCallState.peerConnection;
  }
  
  if (signal.type === 'offer') {
    pc.setRemoteDescription(new RTCSessionDescription(signal.sdp)).then(function() {
      return pc.createAnswer();
    }).then(function(answer) {
      return pc.setLocalDescription(answer);
    }).then(function() {
      send({ type: 'dm_call_signal', to: fromId, signal: { type: 'answer', sdp: pc.localDescription } });
    }).catch(function(err) {
      console.error('Answer error:', err);
    });
  } else if (signal.type === 'answer') {
    pc.setRemoteDescription(new RTCSessionDescription(signal.sdp)).catch(function(err) {
      console.error('Set remote desc error:', err);
    });
  } else if (signal.type === 'candidate' && signal.candidate) {
    pc.addIceCandidate(new RTCIceCandidate(signal.candidate)).catch(function(err) {
      console.error('Add ICE candidate error:', err);
    });
  }
}

function startCallTimer() {
  // Play connected sound
  playCallConnected();
  
  dmCallState.callDuration = 0;
  dmCallState.callTimer = setInterval(function() {
    dmCallState.callDuration++;
    var mins = Math.floor(dmCallState.callDuration / 60);
    var secs = dmCallState.callDuration % 60;
    var statusEl = qS('#dm-call-status');
    if (statusEl) {
      statusEl.textContent = (mins < 10 ? '0' : '') + mins + ':' + (secs < 10 ? '0' : '') + secs;
    }
  }, 1000);
}

function endDMCall() {
  // Stop all call sounds
  stopAllCallSounds();
  
  // Play end sound if call was connected
  if (dmCallState.callTimer) {
    playCallEnded();
  }
  
  // Stop timer
  if (dmCallState.callTimer) {
    clearInterval(dmCallState.callTimer);
    dmCallState.callTimer = null;
  }
  
  // Stop local stream
  if (dmCallState.localStream) {
    dmCallState.localStream.getTracks().forEach(function(track) { track.stop(); });
    dmCallState.localStream = null;
  }
  
  // Close peer connection
  if (dmCallState.peerConnection) {
    dmCallState.peerConnection.close();
    dmCallState.peerConnection = null;
  }
  
  // Remove audio element
  var audio = qS('#dm-call-audio');
  if (audio) audio.remove();
  
  // Reset video
  var localVideo = qS('#dm-local-video');
  var remoteVideo = qS('#dm-remote-video');
  if (localVideo) localVideo.srcObject = null;
  if (remoteVideo) remoteVideo.srcObject = null;
  qS('#dm-call-video-container')?.classList.remove('active');
  
  // Send end signal
  if (dmCallState.peerId) {
    send({ type: 'dm_call_end', to: dmCallState.peerId });
  }
  
  // Reset state
  dmCallState.active = false;
  dmCallState.peerId = null;
  dmCallState.remoteStream = null;
  dmCallState.isMuted = false;
  dmCallState.isVideoEnabled = false;
  dmCallState.isIncoming = false;
  dmCallState.callDuration = 0;
  
  closeModal('dm-call-modal');
  closeModal('incoming-call-modal');
}

function toggleDMCallMute() {
  if (!dmCallState.localStream) return;
  
  dmCallState.isMuted = !dmCallState.isMuted;
  dmCallState.localStream.getAudioTracks().forEach(function(track) {
    track.enabled = !dmCallState.isMuted;
  });
  
  // Play mute/unmute sound
  if (dmCallState.isMuted) {
    playMuteSound();
  } else {
    playUnmuteSound();
  }
  
  var btn = qS('#dm-call-mic');
  if (btn) {
    btn.classList.toggle('active', dmCallState.isMuted);
    btn.querySelector('.mic-on').style.display = dmCallState.isMuted ? 'none' : 'block';
    btn.querySelector('.mic-off').style.display = dmCallState.isMuted ? 'block' : 'none';
  }
}

function toggleDMCallVideo() {
  if (!dmCallState.localStream) return;
  
  var videoTracks = dmCallState.localStream.getVideoTracks();
  if (videoTracks.length === 0) {
    // Need to add video
    navigator.mediaDevices.getUserMedia({ video: true }).then(function(stream) {
      var videoTrack = stream.getVideoTracks()[0];
      dmCallState.localStream.addTrack(videoTrack);
      
      if (dmCallState.peerConnection) {
        dmCallState.peerConnection.addTrack(videoTrack, dmCallState.localStream);
      }
      
      var localVideo = qS('#dm-local-video');
      if (localVideo) localVideo.srcObject = dmCallState.localStream;
      qS('#dm-call-video-container').classList.add('active');
      
      dmCallState.isVideoEnabled = true;
      qS('#dm-call-video-toggle').classList.add('active');
    }).catch(function(err) {
      showNotification('Не удалось включить камеру');
    });
  } else {
    // Toggle existing video
    dmCallState.isVideoEnabled = !dmCallState.isVideoEnabled;
    videoTracks.forEach(function(track) {
      track.enabled = dmCallState.isVideoEnabled;
    });
    
    qS('#dm-call-video-toggle').classList.toggle('active', dmCallState.isVideoEnabled);
    if (!dmCallState.isVideoEnabled) {
      qS('#dm-call-video-container').classList.remove('active');
    } else {
      qS('#dm-call-video-container').classList.add('active');
    }
  }
}

function showIncomingCall(fromId, fromName, fromAvatar, withVideo) {
  var avatarEl = qS('#incoming-call-avatar');
  var nameEl = qS('#incoming-call-name');
  
  if (avatarEl) {
    if (fromAvatar) {
      avatarEl.innerHTML = '<img src="' + fromAvatar + '">';
    } else {
      avatarEl.textContent = fromName.charAt(0).toUpperCase();
    }
  }
  if (nameEl) nameEl.textContent = fromName;
  
  // Store call info
  dmCallState.peerId = fromId;
  dmCallState.isVideoEnabled = withVideo;
  
  // Play ringtone
  playRingtone();
  
  openModal('incoming-call-modal');
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
      
      // Play mute/unmute sound
      if (muted) {
        playMuteSound();
      } else {
        playUnmuteSound();
      }
      
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
    playScreenShareStop();
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
  // Create picker modal - solid background, not transparent
  var overlay = document.createElement('div');
  overlay.className = 'screen-picker-overlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:#1a1a2e;z-index:10000;display:flex;align-items:center;justify-content:center;';
  
  var modal = document.createElement('div');
  modal.style.cssText = 'background:#2d2d44;border-radius:16px;padding:32px;max-width:700px;width:90%;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,0.5);';
  
  var title = document.createElement('h2');
  title.textContent = 'Выберите источник для демонстрации';
  title.style.cssText = 'color:#fff;margin:0 0 8px 0;font-size:24px;font-weight:600;';
  modal.appendChild(title);
  
  var subtitle = document.createElement('p');
  subtitle.textContent = 'Выберите экран или окно которое хотите показать';
  subtitle.style.cssText = 'color:#a0a0a0;margin:0 0 24px 0;font-size:14px;';
  modal.appendChild(subtitle);
  
  var grid = document.createElement('div');
  grid.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:16px;';
  
  sources.forEach(function(source) {
    var item = document.createElement('div');
    item.style.cssText = 'background:#3d3d5c;border-radius:12px;padding:12px;cursor:pointer;transition:all 0.2s;border:3px solid transparent;';
    item.onmouseenter = function() { item.style.borderColor = '#7289da'; item.style.transform = 'scale(1.02)'; };
    item.onmouseleave = function() { item.style.borderColor = 'transparent'; item.style.transform = 'scale(1)'; };
    
    if (source.thumbnail) {
      var thumb = document.createElement('img');
      thumb.src = source.thumbnail.toDataURL();
      thumb.style.cssText = 'width:100%;height:100px;object-fit:cover;border-radius:8px;margin-bottom:10px;background:#1a1a2e;';
      item.appendChild(thumb);
    }
    
    var name = document.createElement('div');
    name.textContent = source.name.length > 25 ? source.name.substring(0, 25) + '...' : source.name;
    name.style.cssText = 'color:#fff;font-size:13px;text-align:center;font-weight:500;';
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
  cancelBtn.style.cssText = 'margin-top:24px;padding:12px 32px;background:#4d4d6d;color:#fff;border:none;border-radius:8px;cursor:pointer;width:100%;font-size:15px;font-weight:500;transition:background 0.2s;';
  cancelBtn.onmouseenter = function() { cancelBtn.style.background = '#5d5d7d'; };
  cancelBtn.onmouseleave = function() { cancelBtn.style.background = '#4d4d6d'; };
  cancelBtn.onclick = function() {
    document.body.removeChild(overlay);
    callback(null);
  };
  modal.appendChild(cancelBtn);
  
  overlay.appendChild(modal);
  
  document.body.appendChild(overlay);
}

function setupScreenShareStream(screenStream) {
  state.screenSharing = true;
  state.screenStream = screenStream;
  
  var voiceScreenBtn = qS('#voice-screen');
  if (voiceScreenBtn) voiceScreenBtn.classList.add('active');
  
  // Make sure we stay in voice view
  showView('voice-view');
  
  // Show local preview
  showLocalScreenPreview(screenStream);
  
  // Play screen share start sound
  playScreenShareStart();
  
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
  video.muted = true; // Local preview is muted
  video.playsInline = true;
  video.style.width = '100%';
  video.style.height = 'auto';
  video.style.display = 'block';
  video.style.background = '#000';
  
  // Create controls overlay
  var controls = document.createElement('div');
  controls.style.cssText = 'position:absolute;bottom:0;left:0;right:0;padding:12px;background:linear-gradient(transparent,rgba(0,0,0,0.9));display:flex;align-items:center;gap:12px;';
  
  // Label
  var label = document.createElement('div');
  label.textContent = 'Демонстрация экрана';
  label.style.cssText = 'color:white;font-size:14px;font-weight:500;flex:1;';
  controls.appendChild(label);
  
  // Volume control (for remote screen shares)
  var volumeContainer = document.createElement('div');
  volumeContainer.style.cssText = 'display:flex;align-items:center;gap:8px;';
  
  var volumeIcon = document.createElement('div');
  volumeIcon.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/></svg>';
  volumeContainer.appendChild(volumeIcon);
  
  var volumeSlider = document.createElement('input');
  volumeSlider.type = 'range';
  volumeSlider.min = '0';
  volumeSlider.max = '100';
  volumeSlider.value = '100';
  volumeSlider.style.cssText = 'width:80px;height:4px;cursor:pointer;accent-color:#7289da;';
  volumeSlider.oninput = function() {
    // This controls volume for remote screen shares
    var remoteVideos = document.querySelectorAll('.remote-screen-video');
    remoteVideos.forEach(function(v) {
      v.volume = volumeSlider.value / 100;
    });
    // Update icon
    if (volumeSlider.value == 0) {
      volumeIcon.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><line x1="23" y1="9" x2="17" y2="15"/><line x1="17" y1="9" x2="23" y2="15"/></svg>';
    } else {
      volumeIcon.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/></svg>';
    }
  };
  volumeContainer.appendChild(volumeSlider);
  controls.appendChild(volumeContainer);
  
  // Stop button
  var stopBtn = document.createElement('button');
  stopBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="white"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>';
  stopBtn.title = 'Остановить демонстрацию';
  stopBtn.style.cssText = 'background:#f04747;border:none;border-radius:6px;padding:8px 12px;cursor:pointer;display:flex;align-items:center;gap:6px;color:white;font-size:12px;';
  stopBtn.onmouseenter = function() { stopBtn.style.background = '#d84040'; };
  stopBtn.onmouseleave = function() { stopBtn.style.background = '#f04747'; };
  stopBtn.onclick = function(e) {
    e.stopPropagation();
    toggleScreenShare();
  };
  controls.appendChild(stopBtn);
  
  screenDiv.appendChild(video);
  screenDiv.appendChild(controls);
  
  // Fullscreen functionality
  var isFullscreen = false;
  var savedStyles = {};
  
  screenDiv.ondblclick = function() {
    if (!isFullscreen) {
      savedStyles = {
        position: screenDiv.style.position,
        width: screenDiv.style.width,
        maxWidth: screenDiv.style.maxWidth,
        borderRadius: screenDiv.style.borderRadius,
        zIndex: screenDiv.style.zIndex
      };
      
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
      label.textContent = 'Двойной клик для выхода';
      isFullscreen = true;
    } else {
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
  
  screenDiv.style.cursor = 'pointer';
  screenDiv.title = 'Двойной клик для полноэкранного режима';
  
  if (voiceUsers.firstChild) {
    voiceUsers.insertBefore(screenDiv, voiceUsers.firstChild);
  } else {
    voiceUsers.appendChild(screenDiv);
  }
  
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
  
  // Show self context menu for yourself
  if (memberId === state.userId) {
    showSelfContext(x, y);
    return;
  }
  
  var ctx = qS('#member-context-full');
  if (!ctx) return;
  
  var srv = state.servers.get(state.currentServer);
  var isOwner = srv && srv.ownerId === state.userId;
  var isMemberOwner = srv && srv.ownerId === memberId;
  var canManageRoles = isOwner || hasPermission('manage_roles');
  var canKick = isOwner || hasPermission('kick');
  var canBan = isOwner || hasPermission('ban');
  var showAdmin = (canManageRoles || canKick || canBan) && !isMemberOwner;
  
  // Show/hide admin actions
  ctx.classList.toggle('show-admin', showAdmin);
  
  positionContextMenu(ctx, x, y);
  ctx.classList.add('visible');
  ctx.dataset.userId = memberId;
  
  // Bind actions
  ctx.querySelector('[data-action="view-profile"]').onclick = function() {
    hideContextMenu();
    showUserProfile(memberId);
  };
  
  ctx.querySelector('[data-action="mention"]').onclick = function() {
    hideContextMenu();
    var member = srv.membersData ? srv.membersData.find(function(m) { return m.id === memberId; }) : null;
    var input = qS('#msg-input');
    if (input && member) {
      input.value += '@' + member.name + ' ';
      input.focus();
    }
  };
  
  ctx.querySelector('[data-action="send-dm"]').onclick = function() {
    hideContextMenu();
    openDM(memberId);
  };
  
  var manageRolesBtn = ctx.querySelector('[data-action="manage-roles"]');
  if (manageRolesBtn) {
    manageRolesBtn.onclick = function(e) {
      e.stopPropagation();
      showRolesSubmenu(e.clientX, e.clientY, memberId);
    };
  }
  
  var kickBtn = ctx.querySelector('[data-action="kick-member"]');
  if (kickBtn) {
    kickBtn.onclick = function() {
      hideContextMenu();
      showConfirmDialog('Исключить пользователя с сервера?', function() {
        send({ type: 'kick_member', serverId: state.currentServer, memberId: memberId });
      });
    };
  }
  
  var banBtn = ctx.querySelector('[data-action="ban-member"]');
  if (banBtn) {
    banBtn.onclick = function() {
      hideContextMenu();
      showConfirmDialog('Забанить пользователя на сервере?', function() {
        send({ type: 'ban_member', serverId: state.currentServer, memberId: memberId });
      });
    };
  }
  
  ctx.querySelector('[data-action="copy-user-id"]').onclick = function() {
    hideContextMenu();
    navigator.clipboard.writeText(memberId).then(function() {
      showNotification('ID скопирован!');
    });
  };
}

function showRolesSubmenu(x, y, memberId) {
  var submenu = qS('#roles-submenu');
  if (!submenu) return;
  
  var srv = state.servers.get(state.currentServer);
  if (!srv || !srv.roles) return;
  
  // Support multiple roles
  var memberRoles = srv.memberRoles ? srv.memberRoles[memberId] : [];
  if (!Array.isArray(memberRoles)) memberRoles = memberRoles ? [memberRoles] : [];
  
  // Sort roles by position (higher position first, like in settings)
  var sortedRoles = srv.roles.slice().sort(function(a, b) {
    return (b.position || 0) - (a.position || 0);
  });
  
  var list = qS('#roles-submenu-list');
  list.innerHTML = sortedRoles.map(function(role) {
    var isAssigned = memberRoles.includes(role.id);
    return '<div class="role-submenu-item" data-role-id="' + role.id + '" data-member-id="' + memberId + '">' +
      '<div class="role-dot" style="background: ' + (role.color || '#99aab5') + '"></div>' +
      '<span class="role-name">' + escapeHtml(role.name) + '</span>' +
      '<div class="role-check ' + (isAssigned ? 'checked' : '') + '"></div>' +
      '</div>';
  }).join('');
  
  // Bind click handlers
  list.querySelectorAll('.role-submenu-item').forEach(function(item) {
    item.onclick = function() {
      var roleId = item.dataset.roleId;
      var targetMemberId = item.dataset.memberId;
      var check = item.querySelector('.role-check');
      var isChecked = check.classList.contains('checked');
      
      if (isChecked) {
        // Remove role
        send({ type: 'assign_role', serverId: state.currentServer, memberId: targetMemberId, roleId: roleId, action: 'remove' });
        check.classList.remove('checked');
        showNotification('Роль снята');
      } else {
        // Add role
        send({ type: 'assign_role', serverId: state.currentServer, memberId: targetMemberId, roleId: roleId });
        check.classList.add('checked');
        showNotification('Роль назначена');
      }
    };
  });
  
  positionContextMenu(submenu, x + 10, y);
  submenu.classList.add('visible');
}

// Styled confirm dialog
function showConfirmDialog(message, onConfirm, onCancel) {
  var overlay = document.createElement('div');
  overlay.className = 'confirm-overlay';
  
  var dialog = document.createElement('div');
  dialog.className = 'confirm-dialog';
  
  var text = document.createElement('div');
  text.className = 'confirm-text';
  text.textContent = message;
  
  var buttons = document.createElement('div');
  buttons.className = 'confirm-buttons';
  
  var confirmBtn = document.createElement('button');
  confirmBtn.className = 'btn primary';
  confirmBtn.textContent = 'Подтвердить';
  confirmBtn.onclick = function() {
    document.body.removeChild(overlay);
    if (onConfirm) onConfirm();
  };
  
  var cancelBtn = document.createElement('button');
  cancelBtn.className = 'btn secondary';
  cancelBtn.textContent = 'Отмена';
  cancelBtn.onclick = function() {
    document.body.removeChild(overlay);
    if (onCancel) onCancel();
  };
  
  buttons.appendChild(confirmBtn);
  buttons.appendChild(cancelBtn);
  dialog.appendChild(text);
  dialog.appendChild(buttons);
  overlay.appendChild(dialog);
  
  document.body.appendChild(overlay);
  
  // Close on overlay click
  overlay.onclick = function(e) {
    if (e.target === overlay) {
      document.body.removeChild(overlay);
      if (onCancel) onCancel();
    }
  };
  
  // Close on Escape
  var escHandler = function(e) {
    if (e.key === 'Escape') {
      document.body.removeChild(overlay);
      document.removeEventListener('keydown', escHandler);
      if (onCancel) onCancel();
    }
  };
  document.addEventListener('keydown', escHandler);
}

function hasPermission(perm) {
  var srv = state.servers.get(state.currentServer);
  if (!srv) return false;
  if (srv.ownerId === state.userId) return true;
  
  var myRoleId = srv.memberRoles ? srv.memberRoles[state.userId] : null;
  if (!myRoleId) return false;
  
  var myRole = srv.roles ? srv.roles.find(function(r) { return r.id === myRoleId; }) : null;
  if (!myRole || !myRole.permissions) return false;
  
  return myRole.permissions.includes(perm) || myRole.permissions.includes('admin') || myRole.permissions.includes('all');
}

function showSelfContext(x, y) {
  var ctx = qS('#self-context');
  if (!ctx) return;
  
  // Update toggle states
  var muteToggle = qS('#ctx-mute-toggle');
  var deafenToggle = qS('#ctx-deafen-toggle');
  if (muteToggle) muteToggle.classList.toggle('active', state.isMuted);
  if (deafenToggle) deafenToggle.classList.toggle('active', state.isDeafened);
  
  positionContextMenu(ctx, x, y);
  ctx.classList.add('visible');
  
  // Bind actions
  ctx.querySelector('[data-action="view-profile"]').onclick = function() {
    hideContextMenu();
    showUserProfile(state.userId);
  };
  
  ctx.querySelector('[data-action="mention-self"]').onclick = function() {
    hideContextMenu();
    var input = qS('#msg-input');
    if (input) {
      input.value += '@' + state.username + ' ';
      input.focus();
    }
  };
  
  ctx.querySelector('[data-action="mute-self"]').onclick = function() {
    state.isMuted = !state.isMuted;
    var muteToggle = qS('#ctx-mute-toggle');
    if (muteToggle) muteToggle.classList.toggle('active', state.isMuted);
    var muteBtn = qS('#mute-btn');
    if (muteBtn) muteBtn.classList.toggle('active', state.isMuted);
    showNotification(state.isMuted ? 'Микрофон выключен' : 'Микрофон включён');
  };
  
  ctx.querySelector('[data-action="deafen-self"]').onclick = function() {
    state.isDeafened = !state.isDeafened;
    var deafenToggle = qS('#ctx-deafen-toggle');
    if (deafenToggle) deafenToggle.classList.toggle('active', state.isDeafened);
    var deafenBtn = qS('#deafen-btn');
    if (deafenBtn) deafenBtn.classList.toggle('active', state.isDeafened);
    showNotification(state.isDeafened ? 'Звук выключен' : 'Звук включён');
  };
  
  ctx.querySelector('[data-action="edit-server-profile"]').onclick = function() {
    hideContextMenu();
    showNotification('Редактирование профиля сервера скоро будет доступно');
  };
  
  ctx.querySelector('[data-action="view-roles"]').onclick = function(e) {
    e.stopPropagation();
    var srv = state.servers.get(state.currentServer);
    var isOwner = srv && srv.ownerId === state.userId;
    var canManage = isOwner || hasPermission('manage_roles');
    
    if (canManage) {
      // Show roles submenu for assigning roles to self
      showRolesSubmenu(e.clientX, e.clientY, state.userId);
    } else {
      hideContextMenu();
      var myRoleId = srv.memberRoles ? srv.memberRoles[state.userId] : null;
      var myRole = myRoleId ? srv.roles.find(function(r) { return r.id === myRoleId; }) : null;
      if (myRole) {
        showNotification('Ваша роль: ' + myRole.name);
      } else {
        showNotification('У вас нет специальных ролей');
      }
    }
  };
  
  ctx.querySelector('[data-action="copy-user-id"]').onclick = function() {
    hideContextMenu();
    navigator.clipboard.writeText(state.userId).then(function() {
      showNotification('ID скопирован!');
    });
  };
}

function showUserProfile(userId) {
  var modal = qS('#user-profile-modal');
  if (!modal) {
    console.error('User profile modal not found');
    return;
  }
  
  // Get user data
  var user = null;
  
  // Check if it's current user
  if (userId === state.userId) {
    user = {
      id: state.userId,
      name: state.username,
      tag: state.userTag,
      avatar: state.userAvatar,
      status: 'online',
      customStatus: state.customStatus,
      createdAt: state.userCreatedAt
    };
  }
  
  // Try to find in server members
  if (!user) {
    var srv = state.servers.get(state.currentServer);
    if (srv && srv.membersData) {
      user = srv.membersData.find(function(m) { return m.id === userId; });
    }
  }
  
  // Try to find in friends
  if (!user) {
    user = state.friends.get(userId);
  }
  
  // Fallback
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
  
  // Set tag
  var tagEl = qS('#profile-tag');
  if (tagEl) tagEl.textContent = '#' + (user.tag || '0000');
  
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
    statusTextEl.className = 'value ' + (user.status || 'offline');
  }
  
  // Set created date
  var createdEl = qS('#profile-created');
  if (createdEl) {
    if (user.createdAt) {
      var date = new Date(user.createdAt);
      var options = { day: 'numeric', month: 'long', year: 'numeric' };
      createdEl.textContent = date.toLocaleDateString('ru-RU', options);
    } else {
      createdEl.textContent = '—';
    }
  }
  
  // Set user roles on current server
  var rolesSection = qS('#profile-roles-section');
  var rolesContainer = qS('#profile-roles');
  if (rolesSection && rolesContainer) {
    var srv = state.servers.get(state.currentServer);
    if (srv && srv.memberRoles && srv.roles) {
      var userRoleIds = srv.memberRoles[userId] || [];
      // Convert to array if it's a single value
      if (!Array.isArray(userRoleIds)) {
        userRoleIds = userRoleIds ? [userRoleIds] : [];
      }
      var userRoles = userRoleIds.map(function(rid) {
        return srv.roles.find(function(r) { return r.id === rid; });
      }).filter(Boolean);
      
      // Sort by position
      userRoles.sort(function(a, b) { return (b.position || 0) - (a.position || 0); });
      
      if (userRoles.length > 0) {
        rolesSection.style.display = 'block';
        rolesContainer.innerHTML = userRoles.map(function(r) {
          return '<div class="profile-role" style="--role-color: ' + (r.color || '#99aab5') + '">' +
            '<span class="role-dot"></span>' + escapeHtml(r.name) + '</div>';
        }).join('');
      } else {
        rolesSection.style.display = 'none';
      }
    } else {
      rolesSection.style.display = 'none';
    }
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
  
  // Bind action buttons (hide for self)
  var dmBtn = qS('#profile-dm-btn');
  var callBtn = qS('#profile-call-btn');
  var friendBtn = qS('#profile-friend-btn');
  var actionsContainer = qS('.profile-actions');
  
  if (actionsContainer) {
    actionsContainer.style.display = userId === state.userId ? 'none' : 'flex';
  }
  
  if (dmBtn) {
    dmBtn.onclick = function() {
      closeModal('user-profile-modal');
      openDM(userId);
    };
  }
  
  if (callBtn) {
    callBtn.onclick = function() {
      closeModal('user-profile-modal');
      startPrivateCall(userId);
    };
  }
  
  if (friendBtn) {
    friendBtn.onclick = function() {
      send({ type: 'friend_request', to: userId });
      showNotification('Запрос в друзья отправлен!');
    };
  }
  
  modal.classList.add('active');
  
  // Close on background click
  modal.onclick = function(e) {
    if (e.target === modal) {
      closeModal('user-profile-modal');
    }
  };
  
  // Close on ESC
  var escHandler = function(e) {
    if (e.key === 'Escape') {
      closeModal('user-profile-modal');
      document.removeEventListener('keydown', escHandler);
    }
  };
  document.addEventListener('keydown', escHandler);
}

function startPrivateCall(userId) {
  // Open DM and start voice call
  openDM(userId);
  startDMCall(userId, false);
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


// ============ PERMISSION HELPERS ============
function setPermissionCheckboxes(perms) {
  // General
  var adminPerm = qS('#perm-admin');
  if (adminPerm) adminPerm.checked = perms.includes('admin') || perms.includes('all');
  var manageRoles = qS('#perm-manage-roles');
  if (manageRoles) manageRoles.checked = perms.includes('manage_roles') || perms.includes('all');
  var manageChannels = qS('#perm-manage-channels');
  if (manageChannels) manageChannels.checked = perms.includes('manage_channels') || perms.includes('all');
  var kick = qS('#perm-kick');
  if (kick) kick.checked = perms.includes('kick') || perms.includes('all');
  var ban = qS('#perm-ban');
  if (ban) ban.checked = perms.includes('ban') || perms.includes('all');
  var manageServer = qS('#perm-manage-server');
  if (manageServer) manageServer.checked = perms.includes('manage_server') || perms.includes('all');
  // Text
  var viewChannels = qS('#perm-view-channels');
  if (viewChannels) viewChannels.checked = perms.includes('view_channels') || perms.includes('read_messages') || perms.includes('all');
  var sendMessages = qS('#perm-send-messages');
  if (sendMessages) sendMessages.checked = perms.includes('send_messages') || perms.includes('all');
  var manageMessages = qS('#perm-manage-messages');
  if (manageMessages) manageMessages.checked = perms.includes('manage_messages') || perms.includes('all');
  var attachFiles = qS('#perm-attach-files');
  if (attachFiles) attachFiles.checked = perms.includes('attach_files') || perms.includes('all');
  var addReactions = qS('#perm-add-reactions');
  if (addReactions) addReactions.checked = perms.includes('add_reactions') || perms.includes('all');
  var mentionEveryone = qS('#perm-mention-everyone');
  if (mentionEveryone) mentionEveryone.checked = perms.includes('mention_everyone') || perms.includes('all');
  var readHistory = qS('#perm-read-history');
  if (readHistory) readHistory.checked = perms.includes('read_history') || perms.includes('all');
  // Voice
  var voiceConnect = qS('#perm-voice-connect');
  if (voiceConnect) voiceConnect.checked = perms.includes('voice_connect') || perms.includes('all');
  var voiceSpeak = qS('#perm-voice-speak');
  if (voiceSpeak) voiceSpeak.checked = perms.includes('voice_speak') || perms.includes('all');
  var voiceVideo = qS('#perm-voice-video');
  if (voiceVideo) voiceVideo.checked = perms.includes('voice_video') || perms.includes('all');
  var voiceStream = qS('#perm-voice-stream');
  if (voiceStream) voiceStream.checked = perms.includes('voice_stream') || perms.includes('all');
  var voiceMute = qS('#perm-voice-mute-members');
  if (voiceMute) voiceMute.checked = perms.includes('voice_mute_members') || perms.includes('all');
  var voiceMove = qS('#perm-voice-move-members');
  if (voiceMove) voiceMove.checked = perms.includes('voice_move_members') || perms.includes('all');
  var voicePriority = qS('#perm-voice-priority');
  if (voicePriority) voicePriority.checked = perms.includes('voice_priority') || perms.includes('all');
}

function getSelectedPermissions() {
  var perms = [];
  // General
  if (qS('#perm-admin') && qS('#perm-admin').checked) perms.push('admin');
  if (qS('#perm-manage-roles') && qS('#perm-manage-roles').checked) perms.push('manage_roles');
  if (qS('#perm-manage-channels') && qS('#perm-manage-channels').checked) perms.push('manage_channels');
  if (qS('#perm-kick') && qS('#perm-kick').checked) perms.push('kick');
  if (qS('#perm-ban') && qS('#perm-ban').checked) perms.push('ban');
  if (qS('#perm-manage-server') && qS('#perm-manage-server').checked) perms.push('manage_server');
  // Text
  if (qS('#perm-view-channels') && qS('#perm-view-channels').checked) perms.push('view_channels');
  if (qS('#perm-send-messages') && qS('#perm-send-messages').checked) perms.push('send_messages');
  if (qS('#perm-manage-messages') && qS('#perm-manage-messages').checked) perms.push('manage_messages');
  if (qS('#perm-attach-files') && qS('#perm-attach-files').checked) perms.push('attach_files');
  if (qS('#perm-add-reactions') && qS('#perm-add-reactions').checked) perms.push('add_reactions');
  if (qS('#perm-mention-everyone') && qS('#perm-mention-everyone').checked) perms.push('mention_everyone');
  if (qS('#perm-read-history') && qS('#perm-read-history').checked) perms.push('read_history');
  // Voice
  if (qS('#perm-voice-connect') && qS('#perm-voice-connect').checked) perms.push('voice_connect');
  if (qS('#perm-voice-speak') && qS('#perm-voice-speak').checked) perms.push('voice_speak');
  if (qS('#perm-voice-video') && qS('#perm-voice-video').checked) perms.push('voice_video');
  if (qS('#perm-voice-stream') && qS('#perm-voice-stream').checked) perms.push('voice_stream');
  if (qS('#perm-voice-mute-members') && qS('#perm-voice-mute-members').checked) perms.push('voice_mute_members');
  if (qS('#perm-voice-move-members') && qS('#perm-voice-move-members').checked) perms.push('voice_move_members');
  if (qS('#perm-voice-priority') && qS('#perm-voice-priority').checked) perms.push('voice_priority');
  return perms;
}

function resetPermissionCheckboxes() {
  // General
  var adminPerm = qS('#perm-admin');
  if (adminPerm) adminPerm.checked = false;
  var manageRoles = qS('#perm-manage-roles');
  if (manageRoles) manageRoles.checked = false;
  var manageChannels = qS('#perm-manage-channels');
  if (manageChannels) manageChannels.checked = false;
  var kick = qS('#perm-kick');
  if (kick) kick.checked = false;
  var ban = qS('#perm-ban');
  if (ban) ban.checked = false;
  var manageServer = qS('#perm-manage-server');
  if (manageServer) manageServer.checked = false;
  // Text - default enabled
  var viewChannels = qS('#perm-view-channels');
  if (viewChannels) viewChannels.checked = true;
  var sendMessages = qS('#perm-send-messages');
  if (sendMessages) sendMessages.checked = true;
  var manageMessages = qS('#perm-manage-messages');
  if (manageMessages) manageMessages.checked = false;
  var attachFiles = qS('#perm-attach-files');
  if (attachFiles) attachFiles.checked = true;
  var addReactions = qS('#perm-add-reactions');
  if (addReactions) addReactions.checked = true;
  var mentionEveryone = qS('#perm-mention-everyone');
  if (mentionEveryone) mentionEveryone.checked = false;
  var readHistory = qS('#perm-read-history');
  if (readHistory) readHistory.checked = true;
  // Voice
  var voiceConnect = qS('#perm-voice-connect');
  if (voiceConnect) voiceConnect.checked = true;
  var voiceSpeak = qS('#perm-voice-speak');
  if (voiceSpeak) voiceSpeak.checked = true;
  var voiceVideo = qS('#perm-voice-video');
  if (voiceVideo) voiceVideo.checked = true;
  var voiceStream = qS('#perm-voice-stream');
  if (voiceStream) voiceStream.checked = true;
  var voiceMute = qS('#perm-voice-mute-members');
  if (voiceMute) voiceMute.checked = false;
  var voiceMove = qS('#perm-voice-move-members');
  if (voiceMove) voiceMove.checked = false;
  var voicePriority = qS('#perm-voice-priority');
  if (voicePriority) voicePriority.checked = false;
}

// ============ EVENT LISTENERS ============
document.addEventListener('DOMContentLoaded', function() {
  // Check for invite code in URL
  var urlParams = new URLSearchParams(window.location.search);
  var inviteCode = urlParams.get('invite');
  if (inviteCode) {
    // Store invite code to use after login
    localStorage.setItem('pendingInvite', inviteCode);
    // Clean URL
    window.history.replaceState({}, document.title, window.location.pathname);
  }
  
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
    send({ type: 'login', email: email, password: pwd });
  };
  
  qS('#reg-btn').onclick = function() {
    var name = qS('#reg-name').value.trim();
    var email = qS('#reg-email').value.trim();
    var pwd = qS('#reg-pass').value;
    if (!name || !email || !pwd) return;
    localStorage.setItem('lastEmail', email);
    localStorage.setItem('lastPwd', pwd);
    send({ type: 'register', email: email, password: pwd, name: name });
  };
  
  var guestBtn = qS('#guest-btn');
  if (guestBtn) {
    guestBtn.onclick = function() {
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
  
  // 2FA Login handlers
  var backToLogin = qS('#back-to-login');
  if (backToLogin) {
    backToLogin.onclick = function(e) {
      e.preventDefault();
      qS('#login-2fa-box').classList.add('hidden');
      qS('#login-box').classList.remove('hidden');
      localStorage.removeItem('lastEmail');
      localStorage.removeItem('lastPwd');
    };
  }
  
  var verifyLoginBtn = qS('#verify-login-btn');
  if (verifyLoginBtn) {
    verifyLoginBtn.onclick = function() {
      var code = '';
      qSA('#login-2fa-box .code-input').forEach(function(inp) {
        code += inp.value;
      });
      if (code.length !== 6) {
        qS('#login-2fa-error').textContent = 'Введите 6-значный код';
        return;
      }
      send({ 
        type: 'login', 
        email: state.pending2FAEmail, 
        password: state.pending2FAPassword,
        loginCode: code
      });
    };
  }
  
  var resendLoginCodeBtn = qS('#resend-login-code-btn');
  if (resendLoginCodeBtn) {
    resendLoginCodeBtn.onclick = function() {
      send({ type: 'resend_login_code', email: state.pending2FAEmail });
    };
  }
  
  // 2FA code input handling
  qSA('#login-2fa-box .code-input').forEach(function(input, index) {
    input.oninput = function(e) {
      var value = e.target.value;
      if (value.length === 1) {
        var next = qS('#login-2fa-box .code-input[data-index="' + (index + 1) + '"]');
        if (next) next.focus();
      }
    };
    input.onkeydown = function(e) {
      if (e.key === 'Backspace' && !input.value && index > 0) {
        var prev = qS('#login-2fa-box .code-input[data-index="' + (index - 1) + '"]');
        if (prev) prev.focus();
      }
      if (e.key === 'Enter') {
        verifyLoginBtn.click();
      }
    };
    input.onpaste = function(e) {
      e.preventDefault();
      var paste = (e.clipboardData || window.clipboardData).getData('text').replace(/\D/g, '');
      if (paste.length === 6) {
        qSA('#login-2fa-box .code-input').forEach(function(inp, i) {
          inp.value = paste[i] || '';
        });
        verifyLoginBtn.click();
      }
    };
  });
  
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
  
  // DM Call buttons
  var dmCallBtn = qS('#dm-call-btn');
  if (dmCallBtn) {
    dmCallBtn.onclick = function() {
      if (!state.currentDM) return;
      startDMCall(state.currentDM, false);
    };
  }
  
  var dmVideoBtn = qS('#dm-video-btn');
  if (dmVideoBtn) {
    dmVideoBtn.onclick = function() {
      if (!state.currentDM) return;
      startDMCall(state.currentDM, true);
    };
  }
  
  // DM Call modal controls
  var dmCallMicBtn = qS('#dm-call-mic');
  if (dmCallMicBtn) {
    dmCallMicBtn.onclick = function() {
      toggleDMCallMute();
    };
  }
  
  var dmCallVideoToggle = qS('#dm-call-video-toggle');
  if (dmCallVideoToggle) {
    dmCallVideoToggle.onclick = function() {
      toggleDMCallVideo();
    };
  }
  
  var dmCallEndBtn = qS('#dm-call-end');
  if (dmCallEndBtn) {
    dmCallEndBtn.onclick = function() {
      endDMCall();
    };
  }
  
  // Incoming call buttons
  var acceptCallBtn = qS('#accept-call-btn');
  if (acceptCallBtn) {
    acceptCallBtn.onclick = function() {
      if (dmCallState.peerId) {
        acceptDMCall(dmCallState.peerId, dmCallState.isVideoEnabled);
      }
    };
  }
  
  var declineCallBtn = qS('#decline-call-btn');
  if (declineCallBtn) {
    declineCallBtn.onclick = function() {
      // Stop ringtone
      stopAllCallSounds();
      playCallEnded();
      
      if (dmCallState.peerId) {
        send({ type: 'dm_call_reject', to: dmCallState.peerId });
        dmCallState.peerId = null;
        dmCallState.isVideoEnabled = false;
        closeModal('incoming-call-modal');
      }
    };
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
      var input = qS('#invite-code').value.trim();
      if (!input) return;
      // Extract code from full URL if needed
      var code = input;
      if (input.includes('?invite=')) {
        code = input.split('?invite=')[1];
      } else if (input.includes('invite=')) {
        code = input.split('invite=')[1];
      }
      send({ type: 'use_invite', code: code });
    };
  }
  
  var inviteCodeInput = qS('#invite-code');
  if (inviteCodeInput) {
    inviteCodeInput.onkeypress = function(e) {
      if (e.key === 'Enter') {
        var input = inviteCodeInput.value.trim();
        if (!input) return;
        // Extract code from full URL if needed
        var code = input;
        if (input.includes('?invite=')) {
          code = input.split('?invite=')[1];
        } else if (input.includes('invite=')) {
          code = input.split('invite=')[1];
        }
        send({ type: 'use_invite', code: code });
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
    closeModal('channel-modal');
  };
  
  // Create category
  var addCategoryBtn = qS('#add-category-btn');
  if (addCategoryBtn) {
    addCategoryBtn.onclick = function() {
      openModal('category-modal');
    };
  }
  
  var createCategoryBtn = qS('#create-category-btn');
  if (createCategoryBtn) {
    createCategoryBtn.onclick = function() {
      var name = qS('#new-category-name').value.trim();
      if (!name || !state.currentServer) return;
      send({ type: 'create_category', serverId: state.currentServer, name: name });
      qS('#new-category-name').value = '';
      closeModal('category-modal');
    };
  }
  
  // Category collapse toggle
  document.addEventListener('click', function(e) {
    var header = e.target.closest('.category-header');
    if (header && !e.target.closest('.add-btn')) {
      var category = header.closest('.channel-category');
      if (category) {
        category.classList.toggle('collapsed');
      }
    }
  });
  
  // Friend request by tag
  qS('#search-btn').onclick = function() {
    var tag = qS('#search-input').value.trim();
    if (!tag) return;
    send({ type: 'friend_request', tag: tag });
    qS('#search-input').value = '';
  };
  
  // Copy my tag
  var copyTagBtn = qS('#copy-my-tag');
  if (copyTagBtn) {
    copyTagBtn.onclick = function() {
      var tag = state.userTag || '0000';
      navigator.clipboard.writeText(tag).then(function() {
        showNotification('ID скопирован!');
      });
    };
  }
  
  // User panel mic toggle
  var userMicBtn = qS('#mic-toggle');
  if (userMicBtn) {
    userMicBtn.onclick = function() {
      if (!state.voiceChannel) {
        showNotification('Вы не в голосовом канале');
        return;
      }
      var muted = toggleMute();
      userMicBtn.classList.toggle('muted', muted);
      
      // Also update voice-mic button if visible
      var voiceMicBtn = qS('#voice-mic');
      if (voiceMicBtn) {
        voiceMicBtn.classList.toggle('muted', muted);
        var micOn = voiceMicBtn.querySelector('.mic-on');
        var micOff = voiceMicBtn.querySelector('.mic-off');
        if (micOn && micOff) {
          micOn.style.display = muted ? 'none' : 'block';
          micOff.style.display = muted ? 'block' : 'none';
        }
      }
    };
  }
  
  // Settings
  qS('#settings-btn').onclick = function() {
    openModal('settings-modal');
    // Reset to first tab
    qSA('#settings-modal .settings-tab').forEach(function(t) { t.classList.remove('active'); });
    qSA('#settings-modal .settings-panel').forEach(function(p) { p.classList.remove('active'); });
    qS('#settings-modal .settings-tab[data-settings="general"]').classList.add('active');
    qS('#settings-general').classList.add('active');
    
    // Fill in values
    qS('#settings-display-name').value = state.username || '';
    qS('#settings-nickname').value = state.username || '';
    qS('#settings-custom-status').value = state.customStatus || '';
    var bioEl = qS('#settings-bio');
    if (bioEl) {
      bioEl.value = state.userBio || '';
      var counter = qS('#bio-counter');
      if (counter) counter.textContent = bioEl.value.length;
    }
    
    var av = qS('#settings-avatar');
    if (av) {
      if (state.userAvatar) av.innerHTML = '<img src="' + state.userAvatar + '">';
      else av.textContent = state.username ? state.username.charAt(0).toUpperCase() : '?';
    }
    
    // Update profile preview
    var nameEl = qS('#profile-preview-name');
    var tagEl = qS('#profile-preview-tag');
    if (nameEl) nameEl.textContent = state.username || 'Пользователь';
    if (tagEl) tagEl.textContent = '#' + (state.userTag || '0000');
    
    var previewBanner = qS('#profile-preview-banner');
    if (previewBanner && state.userBanner) {
      previewBanner.style.backgroundImage = 'url(' + state.userBanner + ')';
    }
    
    // Update email status
    updateEmailStatus();
  };
  
  // Email verification
  function updateEmailStatus() {
    var emailEl = qS('#current-email');
    var badgeEl = qS('#email-badge');
    var emailInput = qS('#settings-email');
    
    if (emailEl) {
      emailEl.textContent = state.verifiedEmail || 'Не привязана';
    }
    if (badgeEl) {
      if (state.emailVerified && state.verifiedEmail) {
        badgeEl.textContent = 'Подтверждена';
        badgeEl.className = 'email-badge verified';
      } else {
        badgeEl.textContent = 'Не подтверждена';
        badgeEl.className = 'email-badge unverified';
      }
    }
    if (emailInput) {
      emailInput.value = '';
      emailInput.placeholder = state.verifiedEmail || 'email@example.com';
    }
  }
  
  // Send verification code
  var sendVerifyBtn = qS('#send-verification-btn');
  if (sendVerifyBtn) {
    sendVerifyBtn.onclick = function() {
      var email = qS('#settings-email').value.trim();
      if (!email) {
        showNotification('Введите email');
        return;
      }
      send({ type: 'send_verification_code', email: email });
    };
  }
  
  // Verify code
  var verifyCodeBtn = qS('#verify-code-btn');
  if (verifyCodeBtn) {
    verifyCodeBtn.onclick = function() {
      var code = '';
      qSA('.code-input').forEach(function(inp) {
        code += inp.value;
      });
      if (code.length !== 6) {
        showNotification('Введите 6-значный код');
        return;
      }
      send({ type: 'verify_email_code', code: code });
    };
  }
  
  // Resend code
  var resendBtn = qS('#resend-code-btn');
  if (resendBtn) {
    resendBtn.onclick = function() {
      send({ type: 'resend_verification_code' });
    };
  }
  
  // Cancel verification
  var cancelVerifyBtn = qS('#cancel-verify-btn');
  if (cancelVerifyBtn) {
    cancelVerifyBtn.onclick = function() {
      qS('#email-verify-form').classList.add('hidden');
      qS('#email-change-form').classList.remove('hidden');
      qSA('.code-input').forEach(function(inp) { inp.value = ''; });
    };
  }
  
  // Code input handling
  qSA('.code-input').forEach(function(input, index) {
    input.oninput = function(e) {
      var value = e.target.value;
      if (value.length === 1) {
        input.classList.add('filled');
        // Move to next input
        var next = qS('.code-input[data-index="' + (index + 1) + '"]');
        if (next) next.focus();
      } else {
        input.classList.remove('filled');
      }
    };
    
    input.onkeydown = function(e) {
      if (e.key === 'Backspace' && !input.value) {
        // Move to previous input
        var prev = qS('.code-input[data-index="' + (index - 1) + '"]');
        if (prev) {
          prev.focus();
          prev.value = '';
          prev.classList.remove('filled');
        }
      }
    };
    
    // Handle paste
    input.onpaste = function(e) {
      e.preventDefault();
      var paste = (e.clipboardData || window.clipboardData).getData('text');
      var digits = paste.replace(/\D/g, '').slice(0, 6);
      digits.split('').forEach(function(digit, i) {
        var inp = qS('.code-input[data-index="' + i + '"]');
        if (inp) {
          inp.value = digit;
          inp.classList.add('filled');
        }
      });
      // Focus last filled or next empty
      var lastIndex = Math.min(digits.length, 5);
      var lastInp = qS('.code-input[data-index="' + lastIndex + '"]');
      if (lastInp) lastInp.focus();
    };
  });
  
  // Bio character counter
  var bioTextarea = qS('#settings-bio');
  if (bioTextarea) {
    bioTextarea.oninput = function() {
      var counter = qS('#bio-counter');
      if (counter) counter.textContent = bioTextarea.value.length;
    };
  }
  
  // Avatar edit overlay
  var avatarOverlay = qS('#avatar-edit-overlay');
  if (avatarOverlay) {
    avatarOverlay.onclick = function() {
      qS('#avatar-input').click();
    };
  }
  
  // Save general settings
  qS('#save-general').onclick = function() {
    var name = qS('#settings-display-name').value.trim();
    if (name) {
      send({ type: 'update_profile', name: name });
      showNotification('Настройки сохранены');
    }
  };
  
  qS('#save-profile').onclick = function() {
    var customStatus = qS('#settings-custom-status').value.trim();
    var bio = qS('#settings-bio').value.trim();
    send({ type: 'update_profile', customStatus: customStatus, bio: bio });
    state.customStatus = customStatus;
    state.userBio = bio;
    showNotification('Профиль сохранён');
  };
  
  // Change password
  var changePassBtn = qS('#change-password-btn');
  if (changePassBtn) {
    changePassBtn.onclick = function() {
      var current = qS('#current-password').value;
      var newPass = qS('#new-password').value;
      var confirm = qS('#confirm-password').value;
      
      if (!current || !newPass || !confirm) {
        showNotification('Заполните все поля');
        return;
      }
      if (newPass !== confirm) {
        showNotification('Пароли не совпадают');
        return;
      }
      if (newPass.length < 8) {
        showNotification('Пароль должен быть минимум 8 символов');
        return;
      }
      
      send({ type: 'change_password', currentPassword: current, newPassword: newPass });
      qS('#current-password').value = '';
      qS('#new-password').value = '';
      qS('#confirm-password').value = '';
    };
  }
  
  // Save notifications
  var saveNotifBtn = qS('#save-notifications');
  if (saveNotifBtn) {
    saveNotifBtn.onclick = function() {
      state.settings = state.settings || {};
      state.settings.notifyMessages = qS('#notify-messages').checked;
      state.settings.notifyMentions = qS('#notify-mentions').checked;
      state.settings.notifyFriends = qS('#notify-friends').checked;
      state.settings.notifySounds = qS('#notify-sounds').checked;
      state.settings.notifyVoiceSounds = qS('#notify-voice-sounds').checked;
      send({ type: 'update_settings', settings: state.settings });
      showNotification('Настройки уведомлений сохранены');
    };
  }
  
  // Save language
  var saveLangBtn = qS('#save-language');
  if (saveLangBtn) {
    saveLangBtn.onclick = function() {
      showNotification('Настройки языка сохранены');
    };
  }
  
  // Logout all devices
  var logoutAllBtn = qS('#logout-all-btn');
  if (logoutAllBtn) {
    logoutAllBtn.onclick = function() {
      showConfirmDialog('Выйти со всех устройств?', function() {
        send({ type: 'logout_all' });
        signOut();
      });
    };
  }
  
  var signoutAllBtn = qS('#signout-all-btn');
  if (signoutAllBtn) {
    signoutAllBtn.onclick = function() {
      showConfirmDialog('Выйти со всех устройств?', function() {
        send({ type: 'logout_all' });
        signOut();
      });
    };
  }
  
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
        var descEl = qS('#edit-server-description');
        if (descEl) descEl.value = srv.description || '';
        var icon = qS('#edit-server-icon');
        if (srv.icon) {
          icon.innerHTML = '<img src="' + srv.icon + '"><div class="avatar-overlay"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg><span>Изменить</span></div>';
          icon.classList.add('has-image');
        } else {
          icon.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg><div class="avatar-overlay"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg><span>Изменить</span></div>';
          icon.classList.remove('has-image');
        }
        // Set privacy radio
        var privacyRadios = qSA('input[name="server-privacy"]');
        privacyRadios.forEach(function(r) {
          r.checked = r.value === (srv.privacy || 'public');
        });
      }
      // Reset to first tab
      qSA('[data-server-settings]').forEach(function(t) { t.classList.remove('active'); });
      qS('[data-server-settings="profile"]').classList.add('active');
      qSA('#server-settings-modal .settings-panel').forEach(function(p) { p.classList.remove('active'); });
      qS('#server-settings-profile').classList.add('active');
      
      openModal('server-settings-modal');
      send({ type: 'get_server_members', serverId: state.editingServerId });
      renderRoles();
      setTimeout(renderServerMembersList, 500);
    };
    serverCtx.querySelector('[data-action="leave"]').onclick = function() {
      var serverId = serverCtx.dataset.serverId;
      showConfirmDialog('Покинуть сервер?', function() {
        send({ type: 'leave_server', serverId: serverId });
      });
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
            var topicEl = qS('#edit-channel-topic');
            if (topicEl) topicEl.value = ch.topic || '';
            var slowmodeEl = qS('#edit-channel-slowmode');
            if (slowmodeEl) slowmodeEl.value = ch.slowmode || '0';
            
            // Show/hide appropriate permissions based on channel type
            var textPerms = qS('#text-channel-perms');
            var voicePerms = qS('#voice-channel-perms');
            var slowmodeGroup = qS('#slowmode-group');
            
            if (isVoice) {
              if (textPerms) textPerms.style.display = 'none';
              if (voicePerms) voicePerms.style.display = 'block';
              if (slowmodeGroup) slowmodeGroup.style.display = 'none';
              qS('#channel-modal-title').textContent = 'Настройки голосового канала';
            } else {
              if (textPerms) textPerms.style.display = 'block';
              if (voicePerms) voicePerms.style.display = 'none';
              if (slowmodeGroup) slowmodeGroup.style.display = 'block';
              qS('#channel-modal-title').textContent = 'Настройки текстового канала';
            }
            
            // Populate role selector
            var roleSelect = qS('#channel-perm-role-select');
            if (roleSelect && srv.roles) {
              roleSelect.innerHTML = '<option value="everyone">@everyone</option>' +
                srv.roles.map(function(r) {
                  return '<option value="' + r.id + '">' + escapeHtml(r.name) + '</option>';
                }).join('');
            }
            
            // Reset to first tab
            qSA('.channel-tab').forEach(function(t) { t.classList.remove('active'); });
            qS('.channel-tab[data-channel-tab="overview"]').classList.add('active');
            qSA('.channel-panel').forEach(function(p) { p.classList.remove('active'); });
            qS('#channel-panel-overview').classList.add('active');
            
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
  
  // Channel tabs
  qSA('.channel-tab').forEach(function(tab) {
    if (tab.id === 'delete-channel-from-settings') return;
    tab.onclick = function() {
      qSA('.channel-tab').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('.channel-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#channel-panel-' + tab.dataset.channelTab);
      if (panel) panel.classList.add('active');
    };
  });
  
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
      var topic = qS('#edit-channel-topic')?.value.trim() || '';
      var slowmode = qS('#edit-channel-slowmode')?.value || '0';
      
      if (name && state.editingChannelId) {
        var channelCtx = qS('#channel-context');
        send({
          type: 'update_channel',
          serverId: state.currentServer,
          channelId: state.editingChannelId,
          name: name,
          topic: topic,
          slowmode: parseInt(slowmode),
          isVoice: channelCtx?.dataset.isVoice === '1'
        });
        closeModal('edit-channel-modal');
        showNotification('Настройки канала сохранены');
      }
    };
  }
  
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
          closeModal('edit-channel-modal');
          openModal('confirm-delete-channel-modal');
        }
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
    var description = qS('#edit-server-description')?.value.trim() || '';
    var privacyRadio = qS('input[name="server-privacy"]:checked');
    var privacy = privacyRadio ? privacyRadio.value : 'public';
    
    if (name && state.editingServerId) {
      send({ 
        type: 'update_server', 
        serverId: state.editingServerId, 
        name: name, 
        icon: state.editServerIcon,
        description: description,
        privacy: privacy
      });
      showNotification('Настройки сохранены');
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
      
      // Also update user panel mic button
      var userMicBtn = qS('#mic-toggle');
      if (userMicBtn) {
        userMicBtn.classList.toggle('muted', muted);
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
    voiceScreenBtn.onclick = function(e) {
      e.stopPropagation();
      e.preventDefault();
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
  
  // Avatar upload
  qS('#upload-avatar').onclick = function() { qS('#avatar-input').click(); };
  qS('#avatar-edit-overlay').onclick = function() { qS('#avatar-input').click(); };
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
  
  // Banner upload
  var uploadBannerBtn = qS('#upload-banner');
  var editBannerBtn = qS('#edit-banner-btn');
  var bannerInput = qS('#banner-input');
  var removeBannerBtn = qS('#remove-banner');
  
  if (uploadBannerBtn) uploadBannerBtn.onclick = function() { bannerInput.click(); };
  if (editBannerBtn) editBannerBtn.onclick = function() { bannerInput.click(); };
  
  if (bannerInput) {
    bannerInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      var reader = new FileReader();
      reader.onload = function(ev) {
        var banner = ev.target.result;
        var previewBanner = qS('#profile-preview-banner');
        if (previewBanner) {
          previewBanner.style.backgroundImage = 'url(' + banner + ')';
        }
        state.userBanner = banner;
        send({ type: 'update_profile', banner: banner });
      };
      reader.readAsDataURL(file);
    };
  }
  
  if (removeBannerBtn) {
    removeBannerBtn.onclick = function() {
      var previewBanner = qS('#profile-preview-banner');
      if (previewBanner) {
        previewBanner.style.backgroundImage = '';
      }
      state.userBanner = null;
      send({ type: 'update_profile', banner: null });
    };
  }
  
  // Update profile preview when settings open
  function updateProfilePreview() {
    var nameEl = qS('#profile-preview-name');
    var tagEl = qS('#profile-preview-tag');
    if (nameEl) nameEl.textContent = state.username || 'Пользователь';
    if (tagEl) tagEl.textContent = '#' + (state.userTag || '0000');
    
    // Update banner if exists
    var previewBanner = qS('#profile-preview-banner');
    if (previewBanner && state.userBanner) {
      previewBanner.style.backgroundImage = 'url(' + state.userBanner + ')';
    }
  }
  
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
      var hexInput = qS('#role-color-hex');
      if (hexInput) hexInput.value = '#99aab5';
      qS('#role-color-preview').style.background = '#99aab5';
      // Reset color presets
      qSA('.color-preset').forEach(function(p) { p.classList.remove('active'); });
      var defaultPreset = qS('.color-preset[data-color="#99aab5"]');
      if (defaultPreset) defaultPreset.classList.add('active');
      // Reset role icon
      var iconPreview = qS('#role-icon-preview');
      if (iconPreview) {
        iconPreview.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>';
      }
      state.roleIcon = null;
      // Reset hoist and mentionable
      var hoistCheckbox = qS('#role-hoist');
      if (hoistCheckbox) hoistCheckbox.checked = false;
      var mentionableCheckbox = qS('#role-mentionable');
      if (mentionableCheckbox) mentionableCheckbox.checked = false;
      // Reset to first tab
      qSA('.role-tab').forEach(function(t) { t.classList.remove('active'); });
      qS('.role-tab[data-role-tab="general"]').classList.add('active');
      qSA('.role-panel').forEach(function(p) { p.classList.remove('active'); });
      qS('#role-panel-general').classList.add('active');
      resetPermissionCheckboxes();
      openModal('role-modal');
    };
  }
  
  // Role tabs
  qSA('.role-tab').forEach(function(tab) {
    tab.onclick = function() {
      qSA('.role-tab').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('.role-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#role-panel-' + tab.dataset.roleTab);
      if (panel) panel.classList.add('active');
    };
  });
  
  // Color presets
  qSA('.color-preset').forEach(function(preset) {
    preset.onclick = function() {
      var color = preset.dataset.color;
      qSA('.color-preset').forEach(function(p) { p.classList.remove('active'); });
      preset.classList.add('active');
      qS('#role-color-input').value = color;
      var hexInput = qS('#role-color-hex');
      if (hexInput) hexInput.value = color;
      qS('#role-color-preview').style.background = color;
    };
  });
  
  var roleColorInput = qS('#role-color-input');
  if (roleColorInput) {
    roleColorInput.oninput = function() {
      var color = roleColorInput.value;
      qS('#role-color-preview').style.background = color;
      var hexInput = qS('#role-color-hex');
      if (hexInput) hexInput.value = color;
      qSA('.color-preset').forEach(function(p) { 
        p.classList.toggle('active', p.dataset.color === color);
      });
    };
  }
  
  var roleColorHex = qS('#role-color-hex');
  if (roleColorHex) {
    roleColorHex.oninput = function() {
      var color = roleColorHex.value;
      if (/^#[0-9A-Fa-f]{6}$/.test(color)) {
        qS('#role-color-input').value = color;
        qS('#role-color-preview').style.background = color;
        qSA('.color-preset').forEach(function(p) { 
          p.classList.toggle('active', p.dataset.color === color);
        });
      }
    };
  }
  
  // Role icon upload
  var uploadRoleIconBtn = qS('#upload-role-icon');
  if (uploadRoleIconBtn) {
    uploadRoleIconBtn.onclick = function() {
      qS('#role-icon-input').click();
    };
  }
  
  var roleIconInput = qS('#role-icon-input');
  if (roleIconInput) {
    roleIconInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      if (file.size > 256 * 1024) {
        showNotification('Файл слишком большой (макс. 256KB)');
        return;
      }
      var reader = new FileReader();
      reader.onload = function(ev) {
        state.roleIcon = ev.target.result;
        var preview = qS('#role-icon-preview');
        if (preview) {
          preview.innerHTML = '<img src="' + ev.target.result + '">';
        }
      };
      reader.readAsDataURL(file);
    };
  }
  
  var saveRoleBtn = qS('#save-role-btn');
  if (saveRoleBtn) {
    saveRoleBtn.onclick = function() {
      var name = qS('#role-name-input').value.trim();
      var color = qS('#role-color-input').value;
      var permissions = getSelectedPermissions();
      var hoist = qS('#role-hoist')?.checked || false;
      var mentionable = qS('#role-mentionable')?.checked || false;
      
      if (!name) {
        showNotification('Введите название роли');
        return;
      }
      
      if (state.editingRoleId) {
        send({ type: 'update_role', serverId: state.editingServerId, roleId: state.editingRoleId, name: name, color: color, permissions: permissions, icon: state.roleIcon, hoist: hoist, mentionable: mentionable });
      } else {
        send({ type: 'create_role', serverId: state.editingServerId, name: name, color: color, permissions: permissions, icon: state.roleIcon, hoist: hoist, mentionable: mentionable });
      }
      closeModal('role-modal');
      showNotification('Роль сохранена');
    };
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
  
  // ============ EXTENDED SERVER SETTINGS ============
  
  // Server settings tabs handler (extended)
  qSA('[data-server-settings]').forEach(function(tab) {
    tab.onclick = function() {
      qSA('[data-server-settings]').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('#server-settings-modal .settings-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#server-settings-' + tab.dataset.serverSettings);
      if (panel) panel.classList.add('active');
      
      // Load data for specific tabs
      var tabName = tab.dataset.serverSettings;
      if (tabName === 'people' || tabName === 'members') {
        loadServerPeople();
      } else if (tabName === 'invites') {
        loadServerInvites();
      } else if (tabName === 'audit') {
        loadAuditLog();
      } else if (tabName === 'bans') {
        loadServerBans();
      }
    };
  });
  
  // Remove server icon
  var removeServerIconBtn = qS('#remove-server-icon');
  if (removeServerIconBtn) {
    removeServerIconBtn.onclick = function() {
      state.editServerIcon = null;
      var icon = qS('#edit-server-icon');
      icon.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg><div class="avatar-overlay"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg><span>Изменить</span></div>';
      icon.classList.remove('has-image');
    };
  }
  
  // Upload areas click handlers
  var emojiUploadArea = qS('#emoji-upload-area');
  if (emojiUploadArea) {
    emojiUploadArea.onclick = function() {
      qS('#emoji-upload-input').click();
    };
  }
  
  var stickerUploadArea = qS('#sticker-upload-area');
  if (stickerUploadArea) {
    stickerUploadArea.onclick = function() {
      qS('#sticker-upload-input').click();
    };
  }
  
  var reactionUploadArea = qS('#reaction-upload-area');
  if (reactionUploadArea) {
    reactionUploadArea.onclick = function() {
      qS('#reaction-upload-input').click();
    };
  }
  
  // Emoji upload handler
  var emojiUploadInput = qS('#emoji-upload-input');
  if (emojiUploadInput) {
    emojiUploadInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      if (file.size > 256 * 1024) {
        showNotification('Файл слишком большой (макс. 256KB)');
        return;
      }
      var reader = new FileReader();
      reader.onload = function(ev) {
        var emojiName = prompt('Введите название эмодзи:', file.name.split('.')[0]);
        if (emojiName) {
          send({ 
            type: 'add_emoji', 
            serverId: state.editingServerId, 
            name: emojiName, 
            image: ev.target.result 
          });
          showNotification('Эмодзи добавлен');
        }
      };
      reader.readAsDataURL(file);
      e.target.value = '';
    };
  }
  
  // Sticker upload handler
  var stickerUploadInput = qS('#sticker-upload-input');
  if (stickerUploadInput) {
    stickerUploadInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      if (file.size > 512 * 1024) {
        showNotification('Файл слишком большой (макс. 512KB)');
        return;
      }
      var reader = new FileReader();
      reader.onload = function(ev) {
        var stickerName = prompt('Введите название стикера:', file.name.split('.')[0]);
        if (stickerName) {
          var category = prompt('Категория (memes, emotions, custom):', 'custom');
          send({ 
            type: 'add_sticker', 
            serverId: state.editingServerId, 
            name: stickerName, 
            category: category || 'custom',
            image: ev.target.result 
          });
          showNotification('Стикер добавлен');
        }
      };
      reader.readAsDataURL(file);
      e.target.value = '';
    };
  }
  
  // Reaction upload handler
  var reactionUploadInput = qS('#reaction-upload-input');
  if (reactionUploadInput) {
    reactionUploadInput.onchange = function(e) {
      var file = e.target.files[0];
      if (!file) return;
      if (file.size > 128 * 1024) {
        showNotification('Файл слишком большой (макс. 128KB)');
        return;
      }
      var reader = new FileReader();
      reader.onload = function(ev) {
        var reactionName = prompt('Введите название реакции:', file.name.split('.')[0]);
        if (reactionName) {
          send({ 
            type: 'add_custom_reaction', 
            serverId: state.editingServerId, 
            name: reactionName, 
            image: ev.target.result 
          });
          showNotification('Реакция добавлена');
          loadCustomReactions();
        }
      };
      reader.readAsDataURL(file);
      e.target.value = '';
    };
  }
  
  // Create invite button in settings
  var createInviteBtn = qS('#create-invite-btn');
  if (createInviteBtn) {
    createInviteBtn.onclick = function() {
      send({ type: 'create_invite', serverId: state.editingServerId });
    };
  }
  
  // Transfer ownership button
  var transferOwnershipBtn = qS('#transfer-ownership-btn');
  if (transferOwnershipBtn) {
    transferOwnershipBtn.onclick = function() {
      showNotification('Функция передачи владения в разработке');
    };
  }
  
  // People search
  var peopleSearch = qS('#people-search');
  if (peopleSearch) {
    peopleSearch.oninput = function() {
      filterPeopleList(peopleSearch.value);
    };
  }
  
  // People filters
  var peopleRoleFilter = qS('#people-role-filter');
  if (peopleRoleFilter) {
    peopleRoleFilter.onchange = function() {
      filterPeopleList(qS('#people-search').value);
    };
  }
  
  var peopleStatusFilter = qS('#people-status-filter');
  if (peopleStatusFilter) {
    peopleStatusFilter.onchange = function() {
      filterPeopleList(qS('#people-search').value);
    };
  }
  
  // Bans search
  var bansSearchInput = qS('#bans-search-input');
  if (bansSearchInput) {
    bansSearchInput.oninput = function() {
      filterBansList(bansSearchInput.value);
    };
  }
  
  // Audit filters
  var auditActionFilter = qS('#audit-action-filter');
  if (auditActionFilter) {
    auditActionFilter.onchange = function() {
      filterAuditLog();
    };
  }
  
  var auditUserFilter = qS('#audit-user-filter');
  if (auditUserFilter) {
    auditUserFilter.onchange = function() {
      filterAuditLog();
    };
  }
  
  // Sticker categories
  qSA('.category-btn').forEach(function(btn) {
    btn.onclick = function() {
      qSA('.category-btn').forEach(function(b) { b.classList.remove('active'); });
      btn.classList.add('active');
      filterStickers(btn.dataset.category);
    };
  });
  
  // Helper functions for server settings
  function loadServerPeople() {
    var srv = state.servers.get(state.editingServerId);
    if (!srv) return;
    
    send({ type: 'get_server_members', serverId: state.editingServerId });
    
    // Update stats
    var members = srv.membersData || [];
    var online = members.filter(function(m) { return m.status === 'online'; });
    
    var totalEl = qS('#total-members-count');
    var onlineEl = qS('#online-members-count');
    var newEl = qS('#new-members-count');
    
    if (totalEl) totalEl.textContent = members.length;
    if (onlineEl) onlineEl.textContent = online.length;
    if (newEl) newEl.textContent = '0'; // Would need join date tracking
    
    // Populate role filter
    var roleFilter = qS('#people-role-filter');
    if (roleFilter && srv.roles) {
      roleFilter.innerHTML = '<option value="all">Все роли</option>';
      srv.roles.forEach(function(role) {
        roleFilter.innerHTML += '<option value="' + role.id + '">' + escapeHtml(role.name) + '</option>';
      });
    }
    
    renderPeopleList(members);
  }
  
  function renderPeopleList(members) {
    var list = qS('#people-list');
    if (!list) return;
    
    if (!members || members.length === 0) {
      list.innerHTML = '<div class="empty-state">Нет участников</div>';
      return;
    }
    
    var h = '';
    members.forEach(function(m) {
      var statusClass = m.status === 'online' ? 'online' : '';
      h += '<div class="person-item" data-id="' + m.id + '" data-role="' + (m.role || 'default') + '" data-status="' + m.status + '">';
      h += '<div class="avatar ' + statusClass + '">' + (m.avatar ? '<img src="' + m.avatar + '">' : (m.name ? m.name.charAt(0).toUpperCase() : '?')) + '</div>';
      h += '<div class="person-info">';
      h += '<div class="person-name">' + escapeHtml(m.name || 'User') + '</div>';
      h += '<div class="person-role">' + escapeHtml(m.role || 'Участник') + '</div>';
      h += '</div>';
      h += '<div class="person-joined">Присоединился недавно</div>';
      h += '</div>';
    });
    
    list.innerHTML = h;
    
    // Click handler for people items
    list.querySelectorAll('.person-item').forEach(function(item) {
      item.onclick = function() {
        openMemberModal(item.dataset.id);
      };
    });
  }
  
  function filterPeopleList(query) {
    var items = qSA('#people-list .person-item');
    var roleFilter = qS('#people-role-filter').value;
    var statusFilter = qS('#people-status-filter').value;
    
    items.forEach(function(item) {
      var name = item.querySelector('.person-name').textContent.toLowerCase();
      var role = item.dataset.role;
      var status = item.dataset.status;
      
      var matchesQuery = !query || name.includes(query.toLowerCase());
      var matchesRole = roleFilter === 'all' || role === roleFilter;
      var matchesStatus = statusFilter === 'all' || status === statusFilter;
      
      item.style.display = (matchesQuery && matchesRole && matchesStatus) ? 'flex' : 'none';
    });
  }
  
  function loadServerInvites() {
    send({ type: 'get_invites', serverId: state.editingServerId });
  }
  
  function renderInvitesList(invites) {
    var list = qS('#invites-list');
    if (!list) return;
    
    if (!invites || Object.keys(invites).length === 0) {
      list.innerHTML = '<div class="empty-state">Активных приглашений нет</div>';
      return;
    }
    
    var baseUrl = window.location.origin + '?invite=';
    var h = '';
    Object.entries(invites).forEach(function(entry) {
      var code = entry[0];
      var data = entry[1];
      var fullLink = baseUrl + code;
      var createdDate = data.createdAt ? new Date(data.createdAt).toLocaleDateString('ru-RU') : 'Неизвестно';
      h += '<div class="invite-item" data-code="' + code + '" data-link="' + fullLink + '">';
      h += '<div class="invite-info">';
      h += '<div class="invite-code">' + fullLink + '</div>';
      h += '<div class="invite-meta">Создан: ' + createdDate + '</div>';
      h += '</div>';
      h += '<div class="invite-actions">';
      h += '<button class="btn secondary copy-invite-btn">Копировать</button>';
      h += '<button class="btn danger-outline delete-invite-btn">Удалить</button>';
      h += '</div>';
      h += '</div>';
    });
    
    list.innerHTML = h;
    
    // Bind handlers
    list.querySelectorAll('.copy-invite-btn').forEach(function(btn) {
      btn.onclick = function() {
        var link = btn.closest('.invite-item').dataset.link;
        navigator.clipboard.writeText(link);
        showNotification('Ссылка скопирована');
      };
    });
    
    list.querySelectorAll('.delete-invite-btn').forEach(function(btn) {
      btn.onclick = function() {
        var code = btn.closest('.invite-item').dataset.code;
        send({ type: 'delete_invite', serverId: state.editingServerId, code: code });
        btn.closest('.invite-item').remove();
        showNotification('Приглашение удалено');
      };
    });
  }
  
  function loadAuditLog() {
    send({ type: 'get_audit_log', serverId: state.editingServerId });
  }
  
  function renderAuditLog(entries) {
    var log = qS('#audit-log');
    if (!log) return;
    
    if (!entries || entries.length === 0) {
      log.innerHTML = '<div class="empty-state">Журнал аудита пуст</div>';
      return;
    }
    
    var h = '';
    entries.forEach(function(entry) {
      var iconClass = '';
      var iconSvg = '';
      
      switch(entry.action) {
        case 'member_join':
          iconClass = 'join';
          iconSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg>';
          break;
        case 'member_leave':
        case 'member_kick':
        case 'member_ban':
          iconClass = 'leave';
          iconSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="18" y1="8" x2="23" y2="13"/><line x1="23" y1="8" x2="18" y2="13"/></svg>';
          break;
        default:
          iconClass = 'update';
          iconSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
      }
      
      h += '<div class="audit-entry" data-action="' + entry.action + '" data-user="' + (entry.userId || '') + '">';
      h += '<div class="audit-icon ' + iconClass + '">' + iconSvg + '</div>';
      h += '<div class="audit-content">';
      h += '<div class="audit-action">' + escapeHtml(entry.description || entry.action) + '</div>';
      h += '<div class="audit-time">' + formatTime(entry.time) + '</div>';
      h += '</div>';
      h += '</div>';
    });
    
    log.innerHTML = h;
  }
  
  function filterAuditLog() {
    var actionFilter = qS('#audit-action-filter').value;
    var userFilter = qS('#audit-user-filter').value;
    
    var entries = qSA('#audit-log .audit-entry');
    entries.forEach(function(entry) {
      var action = entry.dataset.action;
      var user = entry.dataset.user;
      
      var matchesAction = actionFilter === 'all' || action === actionFilter;
      var matchesUser = userFilter === 'all' || user === userFilter;
      
      entry.style.display = (matchesAction && matchesUser) ? 'flex' : 'none';
    });
  }
  
  function loadServerBans() {
    send({ type: 'get_bans', serverId: state.editingServerId });
  }
  
  function renderBansList(bans) {
    var list = qS('#bans-list');
    if (!list) return;
    
    if (!bans || bans.length === 0) {
      list.innerHTML = '<div class="empty-state">Нет забаненных пользователей</div>';
      return;
    }
    
    var h = '';
    bans.forEach(function(ban) {
      h += '<div class="ban-item" data-id="' + ban.id + '">';
      h += '<div class="avatar">' + (ban.name ? ban.name.charAt(0).toUpperCase() : '?') + '</div>';
      h += '<div class="ban-info">';
      h += '<div class="ban-name">' + escapeHtml(ban.name || 'Пользователь') + '</div>';
      h += '<div class="ban-reason">' + escapeHtml(ban.reason || 'Причина не указана') + '</div>';
      h += '</div>';
      h += '<div class="ban-date">' + formatDate(ban.date || Date.now()) + '</div>';
      h += '<button class="btn secondary unban-btn">Разбанить</button>';
      h += '</div>';
    });
    
    list.innerHTML = h;
    
    // Bind unban handlers
    list.querySelectorAll('.unban-btn').forEach(function(btn) {
      btn.onclick = function() {
        var id = btn.closest('.ban-item').dataset.id;
        send({ type: 'unban_member', serverId: state.editingServerId, memberId: id });
        btn.closest('.ban-item').remove();
        showNotification('Пользователь разбанен');
      };
    });
  }
  
  function filterBansList(query) {
    var items = qSA('#bans-list .ban-item');
    items.forEach(function(item) {
      var name = item.querySelector('.ban-name').textContent.toLowerCase();
      item.style.display = (!query || name.includes(query.toLowerCase())) ? 'flex' : 'none';
    });
  }
  
  function loadCustomReactions() {
    var list = qS('#custom-reactions-list');
    if (!list) return;
    
    var srv = state.servers.get(state.editingServerId);
    var reactions = srv?.customReactions || [];
    
    if (reactions.length === 0) {
      list.innerHTML = '<div class="empty-state small">Пользовательские реакции не добавлены</div>';
      return;
    }
    
    var h = '';
    reactions.forEach(function(r) {
      h += '<div class="custom-reaction-item" data-name="' + r.name + '">';
      h += '<img src="' + r.image + '" alt="' + escapeHtml(r.name) + '">';
      h += '<span>:' + escapeHtml(r.name) + ':</span>';
      h += '<button class="delete-btn" title="Удалить">×</button>';
      h += '</div>';
    });
    
    list.innerHTML = h;
    
    list.querySelectorAll('.delete-btn').forEach(function(btn) {
      btn.onclick = function() {
        var name = btn.closest('.custom-reaction-item').dataset.name;
        send({ type: 'remove_custom_reaction', serverId: state.editingServerId, name: name });
        btn.closest('.custom-reaction-item').remove();
      };
    });
  }
  
  function filterStickers(category) {
    var items = qSA('#server-stickers-grid .sticker-item');
    items.forEach(function(item) {
      var itemCategory = item.dataset.category;
      item.style.display = (category === 'all' || itemCategory === category) ? 'flex' : 'none';
    });
  }
  
  function openMemberModal(memberId) {
    state.editingMemberId = memberId;
    var srv = state.servers.get(state.editingServerId);
    if (!srv) return;
    
    var member = (srv.membersData || []).find(function(m) { return m.id === memberId; });
    if (!member) return;
    
    var avatar = qS('#member-modal-avatar');
    var name = qS('#member-modal-name');
    var roleSelect = qS('#member-role-select');
    
    if (avatar) {
      if (member.avatar) {
        avatar.innerHTML = '<img src="' + member.avatar + '">';
      } else {
        avatar.textContent = member.name ? member.name.charAt(0).toUpperCase() : '?';
      }
    }
    
    if (name) name.textContent = member.name || 'Участник';
    
    if (roleSelect && srv.roles) {
      roleSelect.innerHTML = '';
      srv.roles.forEach(function(role) {
        var selected = member.role === role.id ? ' selected' : '';
        roleSelect.innerHTML += '<option value="' + role.id + '"' + selected + '>' + escapeHtml(role.name) + '</option>';
      });
    }
    
    // Hide kick/ban for owner
    var kickBtn = qS('#kick-member-btn');
    var banBtn = qS('#ban-member-btn');
    if (member.isOwner) {
      if (kickBtn) kickBtn.style.display = 'none';
      if (banBtn) banBtn.style.display = 'none';
    } else {
      if (kickBtn) kickBtn.style.display = '';
      if (banBtn) banBtn.style.display = '';
    }
    
    openModal('member-modal');
  }

  // Connect
  connect();
});