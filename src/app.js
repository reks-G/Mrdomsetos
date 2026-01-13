// ============ CONFIG ============
var RENDER_URL = 'wss://discord-clone-ws.onrender.com';
var WS_URL = (window.location.protocol === 'file:' || window.location.hostname === 'localhost' || window.location.hostname === '') 
  ? RENDER_URL 
  : 'wss://' + window.location.hostname;

// ============ STATE ============
var state = {
  ws: null,
  userId: null,
  username: null,
  userAvatar: null,
  servers: new Map(),
  friends: new Map(),
  pendingRequests: [],
  currentServer: null,
  currentChannel: null,
  currentDM: null,
  dmMessages: new Map(),
  dmChats: new Set(),
  voiceChannel: null,
  localStream: null,
  replyingTo: null,
  newServerIcon: null,
  editServerIcon: null,
  editingServerId: null
};

// ============ UTILS ============
function qS(s) { return document.querySelector(s); }
function qSA(s) { return document.querySelectorAll(s); }

function escapeHtml(t) {
  var d = document.createElement('div');
  d.textContent = t;
  return d.innerHTML;
}

function displayStatus(s) {
  var map = { online: 'В сети', idle: 'Не активен', dnd: 'Не беспокоить', invisible: 'Невидимый', offline: 'Не в сети' };
  return map[s] || 'В сети';
}

function formatTime(ts) {
  var d = new Date(ts);
  return d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
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
    auth_success: function() {
      state.userId = msg.userId;
      state.username = msg.user.name;
      state.userAvatar = msg.user.avatar;
      localStorage.setItem('session', JSON.stringify({ userId: msg.userId }));
      
      // Load servers
      if (msg.servers) {
        Object.values(msg.servers).forEach(function(srv) {
          state.servers.set(srv.id, srv);
        });
      }
      
      // Load friends
      if (msg.friends) {
        msg.friends.forEach(function(f) {
          state.friends.set(f.id, f);
          state.dmChats.add(f.id);
        });
      }
      
      // Load pending requests
      if (msg.pendingRequests) {
        state.pendingRequests = msg.pendingRequests;
      }
      
      qS('#auth-screen').classList.remove('active');
      qS('#main-app').classList.remove('hidden');
      updateUserPanel();
      renderServers();
      renderFriends();
      renderDMList();
      loadAudioDevices();
    },
    
    auth_error: function() {
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
        if (ch) ch.name = msg.name;
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
      updateUserPanel();
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
    
    voice_state_update: function() {
      // Update voice users display
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
  if (av) {
    if (state.userAvatar) {
      av.innerHTML = '<img src="' + state.userAvatar + '">';
    } else {
      av.innerHTML = '';
      av.textContent = state.username ? state.username.charAt(0).toUpperCase() : '?';
    }
  }
  if (nm) nm.textContent = state.username || 'Гость';
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
    vh += '<div class="voice-item' + (state.voiceChannel === vc.id ? ' connected' : '') + '" data-id="' + vc.id + '">';
    vh += '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/></svg>';
    vh += '<span>' + escapeHtml(vc.name) + '</span></div>';
  });
  vl.innerHTML = vh;
  
  // Bind events
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
}

function memberHTML(m) {
  var crown = m.isOwner ? '<svg class="crown-icon" viewBox="0 0 24 24" fill="currentColor"><path d="M5 16L3 5l5.5 5L12 4l3.5 6L21 5l-2 11H5zm14 3c0 .6-.4 1-1 1H6c-.6 0-1-.4-1-1v-1h14v1z"/></svg>' : '';
  return '<div class="member-item" data-id="' + m.id + '">' +
    '<div class="avatar ' + (m.status || 'offline') + '">' + (m.avatar ? '<img src="' + m.avatar + '">' : (m.name ? m.name.charAt(0).toUpperCase() : '?')) + '</div>' +
    '<span>' + escapeHtml(m.name || 'User') + crown + '</span></div>';
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
        send({ type: 'friend_accept', from: b.dataset.id });
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
    '<div class="info"><div class="name">' + escapeHtml(u.name || 'User') + '</div><div class="status">' + displayStatus(u.status) + '</div></div>' +
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

// ============ MESSAGES ============
function messageHTML(m) {
  var t = formatTime(m.time || Date.now());
  var a = m.author || 'User';
  var txt = m.text || '';
  var pendingClass = m.pending ? ' pending' : '';
  
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
  
  return '<div class="message' + (m.replyTo ? ' has-reply' : '') + pendingClass + '" data-id="' + m.id + '" data-author-id="' + (m.oderId || '') + '" data-author="' + escapeHtml(a) + '" data-text="' + escapeHtml(txt) + '">' +
    replyHtml +
    '<div class="message-body">' +
    '<div class="avatar">' + (m.avatar ? '<img src="' + m.avatar + '">' : a.charAt(0).toUpperCase()) + '</div>' +
    '<div class="content">' +
    '<div class="header"><span class="author">' + escapeHtml(a) + '</span><span class="time">' + t + '</span></div>' +
    '<div class="text">' + escapeHtml(txt) + '</div>' +
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
  
  // Load history from server
  send({ type: 'get_dm_history', oderId: uid });
  renderDMMessages();
}

function joinVoiceChannel(id) {
  if (state.voiceChannel === id) {
    leaveVoiceChannel();
    return;
  }
  if (state.voiceChannel) leaveVoiceChannel();
  
  state.voiceChannel = id;
  send({ type: 'voice_join', channelId: id, serverId: state.currentServer });
  renderChannels();
  
  var srv = state.servers.get(state.currentServer);
  var ch = srv ? srv.voiceChannels.find(function(c) { return c.id === id; }) : null;
  qS('#voice-name').textContent = ch ? ch.name : 'Голосовой';
  showView('voice-view');
}

function leaveVoiceChannel() {
  send({ type: 'voice_leave', channelId: state.voiceChannel });
  state.voiceChannel = null;
  renderChannels();
  showView('chat-view');
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
function loadAudioDevices() {
  navigator.mediaDevices.enumerateDevices().then(function(devs) {
    var mics = devs.filter(function(d) { return d.kind === 'audioinput'; });
    var spks = devs.filter(function(d) { return d.kind === 'audiooutput'; });
    
    var mo = qS('#mic-select-options');
    var mt = qS('#mic-select-trigger span');
    if (mo && mics.length) {
      var mh = '';
      mics.forEach(function(mic, i) {
        mh += '<div class="custom-select-option' + (i === 0 ? ' selected' : '') + '" data-value="' + mic.deviceId + '">' + (mic.label || 'Микрофон ' + (i + 1)) + '</div>';
      });
      mo.innerHTML = mh;
      if (mt) mt.textContent = mics[0].label || 'Микрофон 1';
    }
    
    var so = qS('#speaker-select-options');
    var st = qS('#speaker-select-trigger span');
    if (so && spks.length) {
      var sh = '';
      spks.forEach(function(spk, i) {
        sh += '<div class="custom-select-option' + (i === 0 ? ' selected' : '') + '" data-value="' + spk.deviceId + '">' + (spk.label || 'Динамик ' + (i + 1)) + '</div>';
      });
      so.innerHTML = sh;
      if (st) st.textContent = spks[0].label || 'Динамик 1';
    }
  }).catch(function(e) {
    console.error('Audio devices error:', e);
  });
}

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
    
    // Optimistic update
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
  qS('#join-server-btn').onclick = function() { openModal('join-modal'); };
  qS('#use-invite-btn').onclick = function() {
    var code = qS('#invite-code').value.trim();
    if (!code) return;
    send({ type: 'use_invite', code: code });
  };
  
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
      qSA('.settings-tab').forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      qSA('.settings-panel').forEach(function(p) { p.classList.remove('active'); });
      var panel = qS('#settings-' + tab.dataset.settings);
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
    channelCtx.querySelector('[data-action="delete-channel"]').onclick = function() {
      if (confirm('Удалить канал?')) {
        send({
          type: 'delete_channel',
          serverId: state.currentServer,
          channelId: channelCtx.dataset.channelId,
          isVoice: channelCtx.dataset.isVoice === '1'
        });
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
    };
    msgCtx.querySelector('[data-action="delete-message"]').onclick = function() {
      send({
        type: 'delete_message',
        serverId: state.currentServer,
        channelId: state.currentChannel,
        messageId: msgCtx.dataset.msgId
      });
    };
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
  qS('#voice-leave').onclick = leaveVoiceChannel;
  
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
  
  // Connect
  connect();
});
