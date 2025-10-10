import { WebSocket, WebSocketServer } from "ws";

type Handler = (data: any, socket: WebSocket) => void;

interface ExtendedSocket extends WebSocket {
  isAlive?: boolean;
}

export class SocketManager {
  private wss: WebSocketServer;
  private events: Record<string, Handler[]> = {};
  private rooms: Map<string, Set<WebSocket>> = new Map();
  private sequenceMap = new Map<WebSocket, number>();
  private userMap = new Map<WebSocket, string>(); // socket se username map
  private onlineUsers = new Set<string>();

  constructor(wss: WebSocketServer) {
    this.wss = wss;

    // When client connects
    this.wss.on("connection", (socket: ExtendedSocket) => {
      console.log("🟢 Client connected");
      socket.isAlive = true;

      // Add client to sequence tracking
      this.addClient(socket);

      // Pong received → mark alive
      socket.on("pong", () => {
        socket.isAlive = true;
      });

      // Handle incoming messages
      socket.on("message", (message) => {
        try {
          const { event, data } = JSON.parse(message.toString());
          const handlers = this.events[event] || [];
          handlers.forEach((fn) => fn(data, socket));
        } catch (err) {
          console.error("❌ Invalid message format:", err);
        }
      });

      // On disconnect
      socket.on("close", () => {
        console.log("❌ Client disconnected");
        this.handleDisconnect(socket);
        this.sequenceMap.delete(socket);
      });
    });

    // Heartbeat (ping/pong)
    setInterval(() => {
      this.wss.clients.forEach((client) => {
        const sock = client as ExtendedSocket;
        if (!sock.isAlive) {
          console.log("💀 Dead socket removed");
          return sock.terminate();
        }
        sock.isAlive = false;
        sock.ping(); // Send ping
      });
    }, 10000);

    // Register event handlers
    this.on("registerUser", (data, socket) => {
      const { username } = data;
      if (!username) return;

      this.userMap.set(socket, username);
      this.onlineUsers.add(username);

      // sabko batao ke user online ho gaya
      this.broadcast("userOnline", { username });

      // naye user ko online users list bhejo
      this.emit(socket, "onlineUsers", Array.from(this.onlineUsers));
    });

    this.on("userTyping", (data, socket) => {
      const { username, typing } = data;
      console.log(`✍️ Typing event received:`, { username, typing, timestamp: new Date().toISOString() });
      
      if (!username) {
        console.log("❌ No username provided in typing event");
        return;
      }

      // Broadcast typing status to all users in the room
      // Note: You might want to limit this to room members only
      console.log(`📢 Broadcasting typing status to all clients:`, { username, typing });
      this.broadcast("userTyping", { username, typing });
    });
  }




  

  // ✅ Add new client and initialize sequence
  private addClient(socket: WebSocket) {
    this.sequenceMap.set(socket, 0);
  }

  private handleDisconnect(socket: WebSocket) {
    const username = this.userMap.get(socket);
    if (username) {
      this.onlineUsers.delete(username);
      this.userMap.delete(socket);
  
      // Notify everyone that this user is offline
      this.broadcast("userOffline", { username });
    }
  
    this.leaveAllRooms(socket);
  }

  // ✅ Get next sequence number for this client
  getNextSequence(socket: WebSocket): number {
    const seq = (this.sequenceMap.get(socket) || 0) + 1;
    this.sequenceMap.set(socket, seq);
    return seq;
  }

  // ✅ (optional) Get current sequence
  getSequence(socket: WebSocket): number {
    return this.sequenceMap.get(socket) || 0;
  }

  // Listen for event
  on(event: string, handler: Handler) {
    if (!this.events[event]) this.events[event] = [];
    this.events[event].push(handler);
  }

  // Send to one client
  emit(socket: WebSocket, event: string, data: any) {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ event, data }));
    }
  }

  // Broadcast to all clients
  broadcast(event: string, data: any) {
    this.wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ event, data }));
      }
    });
  }

  // Join room
  joinRoom(socket: WebSocket, room: string) {
    if (!this.rooms.has(room)) this.rooms.set(room, new Set());
    this.rooms.get(room)!.add(socket);
    console.log(`🏠 Socket joined room: ${room}`);
  }

  // Leave room
  leaveRoom(socket: WebSocket, room: string) {
    this.rooms.get(room)?.delete(socket);
    console.log(`🚪 Socket left room: ${room}`);
  }

  // Leave all rooms
  leaveAllRooms(socket: WebSocket) {
    for (const [room, sockets] of this.rooms.entries()) {
      if (sockets.has(socket)) sockets.delete(socket);
    }
  }

  // Emit message to all sockets in a room
  emitToRoom(room: string, event: string, data: any) {
    const sockets = this.rooms.get(room);
    if (!sockets) return;

    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ event, data }));
      }
    }
  }
}
