// WebSocket连接状态监控模块
class WebSocketStatusMonitor {
  constructor(options = {}) {
    this.statusElement = null;
    this.statusTextElement = null;
    this.websocket = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = options.maxReconnectAttempts || 5;
    this.reconnectDelay = options.reconnectDelay || 3000;
    this.url = options.url || this.getDefaultWebSocketUrl();
    this.isDataReceiving = false;
    this.dataReceivedTimeout = null;
    this.status = 'disconnected'; // disconnected, connecting, connected, receiving
    
    // 初始化状态元素
    this.initElements(options.statusElementId, options.statusTextElementId);
  }

  getDefaultWebSocketUrl() {
    // 根据当前页面URL生成对应的WebSocket URL
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    return `${protocol}//${host}/ws/status`;
  }

  initElements(statusElementId, statusTextElementId) {
    // 如果没有提供元素ID，则创建默认元素
    if (!statusElementId) {
      // 尝试找到最后更新时间的元素
      const timestampElements = document.querySelectorAll('.timestamp');
      if (timestampElements.length > 0) {
        const lastTimestampElement = timestampElements[timestampElements.length - 1];
        const parentElement = lastTimestampElement.parentElement;
        
        // 创建状态圆圈元素
        this.statusElement = document.createElement('span');
        this.statusElement.className = 'websocket-status-indicator';
        
        // 创建状态文本元素
        this.statusTextElement = document.createElement('span');
        this.statusTextElement.className = 'websocket-status-text';
        
        // 插入到时间戳元素旁边
        parentElement.appendChild(this.statusElement);
        parentElement.appendChild(this.statusTextElement);
      }
    } else {
      this.statusElement = document.getElementById(statusElementId);
      this.statusTextElement = document.getElementById(statusTextElementId);
    }
    
    this.updateStatus('disconnected');
  }

  connect() {
    if (this.websocket && (this.websocket.readyState === WebSocket.CONNECTING || this.websocket.readyState === WebSocket.OPEN)) {
      return;
    }

    try {
      this.updateStatus('connecting');
      this.websocket = new WebSocket(this.url);
      
      this.websocket.onopen = () => {
        console.log('WebSocket 连接已建立');
        this.reconnectAttempts = 0;
        this.updateStatus('connected');
      };

      this.websocket.onmessage = (event) => {
        this.handleMessage(event);
      };

      this.websocket.onclose = () => {
        console.log('WebSocket 连接已关闭');
        this.updateStatus('disconnected');
        this.scheduleReconnect();
      };

      this.websocket.onerror = (error) => {
        console.error('WebSocket 错误:', error);
        this.updateStatus('disconnected');
      };
    } catch (error) {
      console.error('WebSocket 连接失败:', error);
      this.updateStatus('disconnected');
      this.scheduleReconnect();
    }
  }

  handleMessage(event) {
    this.updateStatus('receiving');
    this.isDataReceiving = true;
    
    // 3秒后如果没有新数据，回到connected状态
    if (this.dataReceivedTimeout) {
      clearTimeout(this.dataReceivedTimeout);
    }
    
    this.dataReceivedTimeout = setTimeout(() => {
      if (this.status === 'receiving') {
        this.updateStatus('connected');
      }
    }, 3000);
    
    // 这里可以根据需要处理收到的消息数据
    try {
      const data = JSON.parse(event.data);
      // 处理消息数据...
    } catch (e) {
      console.error('解析WebSocket消息失败:', e);
    }
  }

  scheduleReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(`尝试重新连接... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
      
      setTimeout(() => {
        this.connect();
      }, this.reconnectDelay);
    }
  }

  updateStatus(status) {
    this.status = status;
    
    if (!this.statusElement || !this.statusTextElement) {
      return;
    }
    
    // 移除所有状态类
    this.statusElement.className = 'websocket-status-indicator';
    
    // 根据状态更新样式和文本
    switch (status) {
      case 'disconnected':
        this.statusElement.classList.add('status-disconnected');
        this.statusTextElement.textContent = '连接断开';
        break;
      case 'connecting':
        this.statusElement.classList.add('status-connecting');
        this.statusTextElement.textContent = '连接中';
        break;
      case 'connected':
        this.statusElement.classList.add('status-connected');
        this.statusTextElement.textContent = '连接成功';
        break;
      case 'receiving':
        this.statusElement.classList.add('status-receiving');
        this.statusTextElement.textContent = '接收数据';
        break;
    }
  }

  disconnect() {
    if (this.websocket) {
      this.websocket.close();
      this.websocket = null;
    }
    
    if (this.dataReceivedTimeout) {
      clearTimeout(this.dataReceivedTimeout);
      this.dataReceivedTimeout = null;
    }
    
    this.updateStatus('disconnected');
  }

  // 模拟数据接收（用于在没有真实WebSocket连接时测试）
  simulateConnection() {
    // 模拟连接过程
    this.updateStatus('connecting');
    
    setTimeout(() => {
      this.updateStatus('connected');
      
      // 定期模拟数据接收
      setInterval(() => {
        this.updateStatus('receiving');
        setTimeout(() => {
          if (this.status === 'receiving') {
            this.updateStatus('connected');
          }
        }, 2000);
      }, 5000 + Math.random() * 5000);
    }, 1500);
  }
}

// 添加CSS样式
const addWebSocketStatusStyles = () => {
  const style = document.createElement('style');
  style.textContent = `
    .websocket-status-indicator {
      display: inline-block;
      width: 12px;
      height: 12px;
      border-radius: 50%;
      margin: 0 8px;
      vertical-align: middle;
    }
    
    .websocket-status-text {
      font-size: 0.85rem;
      color: #94a3b8;
      margin-left: 4px;
      vertical-align: middle;
    }
    
    /* 断开状态 - 红色 */
    .status-disconnected {
      background-color: #ef4444;
      animation: none;
    }
    
    /* 连接中 - 黄色 */
    .status-connecting {
      background-color: #f59e0b;
      animation: pulse 1.5s infinite;
    }
    
    /* 连接成功 - 绿色 */
    .status-connected {
      background-color: #10b981;
      animation: none;
    }
    
    /* 接收数据 - 蓝色闪烁 */
    .status-receiving {
      background-color: #3b82f6;
      animation: blink 1s infinite;
    }
    
    @keyframes pulse {
      0% {
        opacity: 1;
        transform: scale(1);
      }
      50% {
        opacity: 0.6;
        transform: scale(0.9);
      }
      100% {
        opacity: 1;
        transform: scale(1);
      }
    }
    
    @keyframes blink {
      0%, 100% {
        opacity: 1;
      }
      50% {
        opacity: 0.4;
      }
    }
  `;
  document.head.appendChild(style);
};

// 初始化函数
export const initWebSocketStatus = (options = {}) => {
  addWebSocketStatusStyles();
  
  // 由于可能没有真实的WebSocket服务，我们使用模拟模式
  const monitor = new WebSocketStatusMonitor(options);
  
  // 使用模拟连接（在真实环境中可以替换为monitor.connect()）
  monitor.simulateConnection();
  
  // 监听页面卸载时断开连接
  window.addEventListener('beforeunload', () => {
    monitor.disconnect();
  });
  
  return monitor;
};