const { app, BrowserWindow, ipcMain, Menu, Tray, nativeImage, shell } = require('electron');
const path = require('path');

let mainWindow;
let tray;

// Локальный сервер отключен - используем Render сервер
// function startServer() { ... }

function createWindow() {
  // Отключаем кэш
  const { session } = require('electron');
  session.defaultSession.clearCache();
  
  // Create simple icon using nativeImage
  const size = 64;
  let appIcon;
  
  try {
    // Create a simple colored square icon
    const iconBuffer = Buffer.alloc(size * size * 4);
    
    for (let y = 0; y < size; y++) {
      for (let x = 0; x < size; x++) {
        const idx = (y * size + x) * 4;
        // Calculate distance from center for circle
        const dx = x - size/2;
        const dy = y - size/2;
        const dist = Math.sqrt(dx*dx + dy*dy);
        
        if (dist < size/2 - 2) {
          // Inside circle - gradient purple to blue
          const t = x / size;
          iconBuffer[idx] = Math.floor(168 * (1-t) + 59 * t);     // R
          iconBuffer[idx + 1] = Math.floor(85 * (1-t) + 130 * t); // G
          iconBuffer[idx + 2] = Math.floor(247 * (1-t) + 246 * t); // B
          iconBuffer[idx + 3] = 255; // A
          
          // Draw M letter (simplified)
          const cx = size/2;
          const cy = size/2;
          const letterSize = size * 0.5;
          const lx = x - cx;
          const ly = y - cy;
          
          // M shape detection (very simplified)
          if (Math.abs(ly) < letterSize/2) {
            if (Math.abs(lx + letterSize/3) < 3 || // left vertical
                Math.abs(lx - letterSize/3) < 3 || // right vertical
                (ly < 0 && Math.abs(lx - ly * 0.5) < 3) || // left diagonal
                (ly < 0 && Math.abs(lx + ly * 0.5) < 3)) { // right diagonal
              iconBuffer[idx] = 255;
              iconBuffer[idx + 1] = 255;
              iconBuffer[idx + 2] = 255;
            }
          }
        } else {
          // Outside circle - transparent
          iconBuffer[idx] = 0;
          iconBuffer[idx + 1] = 0;
          iconBuffer[idx + 2] = 0;
          iconBuffer[idx + 3] = 0;
        }
      }
    }
    
    appIcon = nativeImage.createFromBuffer(iconBuffer, { width: size, height: size });
  } catch (e) {
    console.log('Could not create icon:', e.message);
    appIcon = null;
  }
  
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
    icon: appIcon
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
  // Create simple icon for tray
  const size = 32;
  let icon;
  
  try {
    const iconBuffer = Buffer.alloc(size * size * 4);
    
    for (let y = 0; y < size; y++) {
      for (let x = 0; x < size; x++) {
        const idx = (y * size + x) * 4;
        const dx = x - size/2;
        const dy = y - size/2;
        const dist = Math.sqrt(dx*dx + dy*dy);
        
        if (dist < size/2 - 1) {
          const t = x / size;
          iconBuffer[idx] = Math.floor(168 * (1-t) + 59 * t);
          iconBuffer[idx + 1] = Math.floor(85 * (1-t) + 130 * t);
          iconBuffer[idx + 2] = Math.floor(247 * (1-t) + 246 * t);
          iconBuffer[idx + 3] = 255;
        } else {
          iconBuffer[idx] = 0;
          iconBuffer[idx + 1] = 0;
          iconBuffer[idx + 2] = 0;
          iconBuffer[idx + 3] = 0;
        }
      }
    }
    
    icon = nativeImage.createFromBuffer(iconBuffer, { width: size, height: size });
    if (icon.isEmpty()) icon = nativeImage.createEmpty();
  } catch (e) {
    icon = nativeImage.createEmpty();
  }
  
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

// Screen share support
ipcMain.handle('get-screen-sources', async () => {
  const { desktopCapturer } = require('electron');
  const sources = await desktopCapturer.getSources({ 
    types: ['window', 'screen'],
    thumbnailSize: { width: 150, height: 150 }
  });
  return sources;
});

app.whenReady().then(() => {
  // Локальный сервер отключен - десктоп использует Render сервер
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
