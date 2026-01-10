const { app, BrowserWindow, ipcMain, Menu, Tray, nativeImage, shell } = require('electron');
const path = require('path');

let mainWindow;
let tray;

// Запуск встроенного сервера
function startServer() {
  const net = require('net');
  const PORT = 3001;
  
  // Check if port is already in use
  const tester = net.createServer()
    .once('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        console.log(`Port ${PORT} already in use, server might be running`);
      }
    })
    .once('listening', () => {
      tester.close(() => {
        try {
          require('./server.js');
          console.log('WebSocket server started');
        } catch (e) {
          console.error('Server error:', e);
        }
      });
    })
    .listen(PORT);
}

function createWindow() {
  // Отключаем кэш
  const { session } = require('electron');
  session.defaultSession.clearCache();
  
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 800,
    minWidth: 940,
    minHeight: 600,
    frame: false,
    titleBarStyle: 'hidden',
    backgroundColor: '#1a1a2e',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    },
    icon: path.join(__dirname, 'assets', 'icon.png')
  });

  mainWindow.loadFile('src/index.html');

  // DevTools в режиме разработки
  if (process.argv.includes('--dev')) {
    mainWindow.webContents.openDevTools();
  }

  mainWindow.on('close', (e) => {
    if (!app.isQuitting) {
      e.preventDefault();
      mainWindow.hide();
    }
  });
}

function createTray() {
  const icon = nativeImage.createEmpty();
  tray = new Tray(icon);
  
  const contextMenu = Menu.buildFromTemplate([
    { label: 'Открыть MrDomestos*', click: () => mainWindow.show() },
    { type: 'separator' },
    { label: 'Выход', click: () => { app.isQuitting = true; app.quit(); } }
  ]);
  
  tray.setToolTip('MrDomestos*');
  tray.setContextMenu(contextMenu);
  tray.on('click', () => mainWindow.show());
}

// IPC handlers для управления окном
ipcMain.on('window-minimize', () => mainWindow.minimize());
ipcMain.on('window-maximize', () => {
  if (mainWindow.isMaximized()) mainWindow.unmaximize();
  else mainWindow.maximize();
});
ipcMain.on('window-close', () => mainWindow.hide());

// Открытие ссылок в браузере
ipcMain.on('open-external', (_, url) => shell.openExternal(url));

app.whenReady().then(() => {
  startServer();
  createWindow();
  createTray();
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
  else mainWindow.show();
});

// Предотвращаем множественные экземпляры
const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on('second-instance', () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.show();
      mainWindow.focus();
    }
  });
}
